import re
import pandas as pd
import numpy as np
from utils.io import DBIO
from itertools import chain


"""
HTS Class managers inheritance structure:

HouseholdManagerClass: Contains dict for all persons in the household
├─── {person_id: PersonManagerClass} - Inherits household, contains dict for all days for a person
|               ├── {day_id: DayManagerClass} - Inherits person, contains dict for all tours for a day
|               |           ├── {tour_id: TourManagerClass} - Iherits day, contains dict for all trips for a tour
|               |           |              ├── {trip_id: TripManagerClass} - Inherits tour, contains trip data
...

This enables tree-like traversal of the data so only the relevant data is available at each iteration.
E.g., from a trip, person data can be accessed via TripMgr.TourMgr.DayMgr.PersonMgr.data
which will return the person data for the person who made the trip.

We're going to skip Tour manager for now until it can be fully implemented.

"""

class ManagerClass:
    """
    This is a base-manager class to avoid repeating myself, 
    which gets inherited by all other table-specific manager classes.
    
    It contains the DBIO object, which is used to fetch and update data from the "database",
    so there is no need to re-reference the DBIO object in sub managers, it is inherited here.

    """
    
    
    # The Database IO object is part of the class
    # DBIO: utils.io.IO = DBIO
    
    def __init__(self, data: pd.DataFrame|pd.Series|None) -> None:
        
        assert isinstance(data, pd.DataFrame|None), 'Class manager data must be a pandas dataframe'
        
        if isinstance(data, pd.DataFrame):
            assert data.shape[0] == 1, 'Class manager data must be a single row pandas dataframe'
            index = data.index.name     
            data = data.reset_index().squeeze(axis=0)
            assert isinstance(data, pd.Series), 'Class manager data must be a pandas series when not None'
            data.name = index
            
        self.data = data
                        
            
    def get_related(self, related: str, on: str|list|None = None) -> pd.DataFrame:
        """
        Fetches related data from the class manager data

        Args:
            related (str): desired related data table name, must have column with a common index with data
            on (str): column name to join on, default is index name

        Returns:
            pd.DataFrame: related data table
        """
        df = DBIO.get_table(related)
        
        assert isinstance(df, pd.DataFrame), f'{related} table is not a DataFrame'        
        assert isinstance(self.data, pd.Series), 'Class manager data must be a pandas se when fetching related data'
        assert isinstance(on, str|list) or on is None, 'Class manager related data on must be a string, list, or None'
        
        if on is None:
            assert self.data.name in df.columns, 'Class manager related data must have a common index with data'            
            # on_cols = self.data.index.name
            # on_vals = self.data.index            
            on_cols = self.data.name
            assert isinstance(on_cols, str), 'Class manager data must have a single index name'
            on_vals = [self.data[on_cols]]            
            related_df = df[df[on_cols].isin(on_vals)]
            
        else:            
            on = on if isinstance(on, list) else [on]
            assert all(isinstance(s, str) for s in on), 'Class manager related data on must be a list of strings'                
            
            on_vals = self.data[on]
                        
            related_df = df[df.reset_index()[on_vals.index].eq(on_vals).all(axis=1).values]
            # related_df = pd.merge(df.reset_index(), on_vals, on=on).set_index(df.index.name)            
        
        assert isinstance(related_df, pd.DataFrame), 'Class manager related data must be a pandas DataFrame'
        return related_df
            
    def populate(self, host_record: pd.Series, table: str, reserved_cols: list, actions: dict) -> pd.Series:
        """
        Populates a record with values from the host/empty record and the class manager data.
        
        It will cycle through the named methods in each class manager to apply the action to the host record.

        Args:
            host_record (pd.Series): Either a host or empty dummy record to be populated/updated.

        Returns:
            pd.Series: The populated record.
        """
        
        # Copy host to avoid overwriting
        new_record = host_record.copy()
        
        col_check = list(chain(*actions.values()))    
        missing = set(new_record.index).difference(set(col_check + reserved_cols))
        # assert len(missing) == 0, f'Columns {missing} not in imputation config table' 

        # Create locals dict to pass into the populate_column method
        locals_dict = {'host_record': host_record}            
          
        # Run methods batch-wise on columns to enable multi-column coordination (e.g., depart/arrive time)        
        field_values = {}
        for method, fields in actions.items():
            # print(f'Populating {fields.values} with {method}')
            if ':' in method:
                method, locals_dict['from_field'] = method.split(':')
            else:
                locals_dict.pop('from_field', None)        
            field_values.update(self.populate_columns(fields.to_list(), method, **locals_dict))
            
        field_values = pd.Series(field_values)
        
        # Update the new trip with the values
        new_record.update(field_values)

        return new_record
        
    
    def populate_columns(self, fields: list, method: str, **kwargs) -> dict:
        """
        Populates multiple columns in a record with values from the class manager method.

        Args:
            fields (list): The list of column names to populate
            method (str): The name of the method to call

        Returns:
            dict: The dictionary of column names and values
        """
        # Update locals dict
        kwargs['fields'] = fields
        
        assert method != 'evaluate_expression', f'"evaluate_expression" is a reserved method name'
        
        # Check if the method is a directly set value by expression
        if hasattr(self, method):
            values = getattr(self, method)(**kwargs)
        else:                 
            values = self.evaluate_expression(method, **kwargs)
        
        assert isinstance(values, dict), f'Invalid return type {type(values)} for method {method}'                
        
        return values
    
    def evaluate_expression(self, method: str, **kwargs) -> dict:
        """
        Check if the method has a direct set value argument.
        If so, return the value, otherwise return None

        Args:
            method (str): string name of the method

        Returns:
            str|int|float|None: The value to set the column to or None
        """
        
        match = re.search(r'(str\(|int\(|float\(|np.)', method)        
        
        assert match is not None, f'Invalid expression {method}'
        
        return {k: eval(method) for k in kwargs['fields']}        

        
        