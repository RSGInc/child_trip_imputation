import re
import pandas as pd
import numpy as np

# Local imports
import settings
from utils.misc import cat_joint_trip_id, cat_trip_id


# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
assert isinstance(settings.IMPUTATION_CONFIGS, dict), 'IMPUTATION_CONFIGS not a dict'

COLNAMES = settings.COLUMN_NAMES

PER_ID_NAME = COLNAMES['PER_ID_NAME']
TRIP_ID_NAME = COLNAMES['TRIP_ID_NAME']
HH_ID_NAME = COLNAMES['HH_ID_NAME']
DAYNUM_COL = COLNAMES['DAYNUM']
TRIPNUM_COL = COLNAMES['TRIPNUM']
TRAVELDATE_COL = COLNAMES['TRAVELDATE']
DRIVER_COL = COLNAMES['DRIVER']
JOINT_TRIP_ID_NAME = COLNAMES['JOINT_TRIP_ID_NAME']
JOINT_TRIPNUM_COL = COLNAMES['JOINT_TRIPNUM']
COL_ACTIONS_PATH = settings.IMPUTATION_CONFIGS.get('impute_reported_joint_trips')

assert isinstance(COL_ACTIONS_PATH, str), 'impute_reported_joint_trips not a string'

class NonProxyTripPopulator:
    """
    Create a new trip from a host trip and a non-proxy household member, 
    the attributes are populated using the defined class methods listed in the trip_column_actions.csv file
    """
    
    def __init__(self, person_df: pd.DataFrame, trips_df: pd.DataFrame) -> None:
        assert isinstance(COL_ACTIONS_PATH, str), f'COL_ACTIONS_PATH {COL_ACTIONS_PATH} not a string'
        
        self.actions = pd.read_csv(COL_ACTIONS_PATH)
        self.trips_df = trips_df
        self.person_df = person_df
        
        # Trip counters
        # Keeps track of the latest trip number and trip ID for each person
        trip_grp = trips_df.reset_index().groupby([PER_ID_NAME])
        joint_trip_grp = trips_df.reset_index().groupby([HH_ID_NAME])    
            
        self.trip_counter = trip_grp.aggregate({TRIPNUM_COL: 'max', TRIP_ID_NAME: 'max'})
        self.joint_trip_counter = joint_trip_grp.aggregate({JOINT_TRIPNUM_COL: 'max', JOINT_TRIP_ID_NAME: 'max'})

    def populate(self, host_trip: pd.DataFrame, member_id: int|str, member_num: int|str) -> pd.DataFrame:         
        
        # Copy host trip and update column value
        new_trip = host_trip.copy()
        
        # Reserved columns - meaning they are not checked for in the trip_column_actions.csv file
        reserved_cols = [JOINT_TRIPNUM_COL, JOINT_TRIP_ID_NAME, 'tour_id', 'tour_num', 'tour_type']
        
        missing = set(new_trip.index).difference(set(self.actions.colname.to_list() + reserved_cols))
        assert len(missing) == 0, f'Columns {missing} not in trip_column_actions.csv'        

        locals_dict = {'host_trip': host_trip, 'member_id': member_id, 'member_num': member_num}            
        
        # Loops through each column and applies the method to the host trip
        # Using dict comprehension because it is byte compiled and almost twice as fast as standard for loop
        values = {colname: self.populate_column(colname, method, **locals_dict) for i, colname, method in self.actions.itertuples()}        
        values = pd.Series(values)
        
        # Update the new trip with the values
        new_trip.update(values)        
                    
        # Update the trip id        
        # Simply concatenating the ID is problematic because the trip number is inconsistent with trip_num
        # Need to pull the new trip_id from the trip_counter and joint_trip_counter
        assert isinstance(TRIP_ID_NAME, str), f'TRIP_ID_NAME {TRIP_ID_NAME} not a string'
        
        new_trip_id = self.trip_counter.loc[member_id, TRIP_ID_NAME]
        new_joint_trip_id = self.joint_trip_counter.loc[new_trip[HH_ID_NAME], JOINT_TRIP_ID_NAME]
        
        assert new_trip_id not in self.trips_df.index, f'Trip id {new_trip_id} already exists'
        assert new_joint_trip_id not in self.trips_df[JOINT_TRIP_ID_NAME], f'Joint trip id {new_joint_trip_id} already exists'
        
        new_trip[JOINT_TRIP_ID_NAME] = new_joint_trip_id
        new_trip.name = new_trip_id
        
        return new_trip
    
    def populate_column(self, colname, method: str, **kwargs) -> str|int|float|None:
        # Update locals dict
        kwargs['colname'] = colname            
        
        # Check if the method has a direct set value argument
        value = self.set_value(method)
        
        # otherwise call the method
        if value is None:
            assert hasattr(self, method), f'No method {method} found'
            value = getattr(self, method)(**kwargs)
            
        return value
    
    def set_value(self, method: str) -> str|int|float|None:
        """
        Check if the method has a direct set value argument.
        If so, return the value, otherwise return None

        Args:
            method (str): string name of the method

        Returns:
            str|int|float|None: The value to set the column to or None
        """
        
        match = re.search(r'(str\(|int\(|float\(|np.)', method)        
        
        if match is not None:
            return eval(method)
        else: 
            return None
    
    def iterate_counter(self, counter_name: str, counter_id: int|str) -> int:        
        """
        Find the current max trip number and iterate.
        
        Args:
            num_item (str): ['joint_trip', 'trip'] The name of the trip number column to iterate
            item_id (int|str): The household id for the joint_trip_counter or the household member person id

        Returns:
            int: The new trip number
        """
        
        assert counter_name in ['joint_trip', 'trip'], f'num_item must be one of {["joint_trip", "trip"]}'
        
        if counter_name == 'joint_trip':
            assert len(str(counter_id)) == 8, 'joint trip household id must be 10 digits, is this the wrong id?'
            id_method = cat_joint_trip_id
        else:
            assert len(str(counter_id)) == 10, 'trip person id must be 10 digits, is this the wrong id?'
            id_method = cat_trip_id

        # Dictionary of the column names
        params = {
            'trip': [TRIP_ID_NAME, TRIPNUM_COL],
            'joint_trip': [JOINT_TRIP_ID_NAME, JOINT_TRIPNUM_COL]
            }        
        counter_index_name, counter_num_col = params[counter_name]
                
        # Retrieve the trip counter        
        counter_df = getattr(self, counter_name + '_counter')
                
        # Initialize the counter if it doesn't exist
        if counter_id not in counter_df.index:
            newindex = pd.Index([counter_id], name=counter_df.index.name)
            newindex = counter_df.index.append(newindex)
            counter_df = counter_df.reindex(newindex, fill_value=995)            
            
        # Iterate
        counter_num_id, count = counter_df.loc[counter_id, [counter_index_name, counter_num_col]]
        
        # If count is NAN or 995, then it is a new trip and set to 1
        count = 1 if (np.isnan(count) or count == 995 or count < 0) else count + 1
        
        # If counter_num_id is NAN or 995, then it is a new trip and needs to be generated
        if (np.isnan(counter_num_id) or counter_num_id == 995 or counter_num_id < 0): 
            row = pd.Series([counter_id, count], index=[counter_df.index.name, counter_num_col])
            counter_num_id = id_method(row)
        else:
            counter_num_id += 1
        
        # Update the count
        counter_df.loc[counter_id] = count, counter_num_id        
                
        # Return the new trip number
        setattr(self, counter_name + '_counter', counter_df)
                                  
        return count
        
    def copy_from_host(self, **kwargs):
        """
        Although host_trip has already been copied, this method ensures that every column is explicitly defined in teh trip_column_actions.csv file.        
        """
        assert kwargs['colname'] in kwargs['host_trip'].index, f'Column {kwargs["colname"]} does not exist in host trip'
        
        return kwargs['host_trip'][kwargs['colname']]

    def copy_from_member(self, **kwargs) -> int | str | np.integer:
        """
        Copy the value from the member's trip to the new trip.

        Returns:
            int|str: returned value from the member's trip
        """
        
        if self.person_df.index.name == kwargs['colname']:
            return kwargs['member_id']
        
        data = self.person_df.loc[kwargs['member_id'], kwargs['colname']]

        err_msg = f'Column {kwargs["colname"]} has more than one value for person {kwargs["member_id"]}'
        assert data.size == 1, err_msg        
        assert isinstance(data, int | str | np.integer), f'Column {kwargs["colname"]} is not a string or integer'

        return data
    
    def update_trip_num(self, **kwargs) -> int|str: 
        """
        Update the trip number for the new trip.

        Returns:
            int|str: returns the new trip number
        """
        
        member_id = kwargs['member_id']
        # day_num = kwargs['host_trip'][DAYNUM_COL]
        
        return self.iterate_counter('trip', member_id)
    
    def update_first_date(self, **kwargs):
        """
        Update the first date for the new trip. Takes the minimum date from the member's trips and the host trip.

        Returns:
            datetime object: The first trip date
        """
        is_person = self.trips_df[PER_ID_NAME] == kwargs['member_id']
        dates = np.array(self.trips_df.loc[is_person, 'travel_date'])
        
        # Get minimum date against persons trips and the new host trip
        first_date = np.append(dates, kwargs['host_trip'][TRAVELDATE_COL]).min()
            
        return first_date
    
    def update_last_date(self, **kwargs):
        """
        Update the last date for the new trip. Takes the maximum date from the member's trips and the host trip.

        Returns:
            datetime object: The last trip date
        """
        is_person = self.trips_df[PER_ID_NAME] == kwargs['member_id']
        dates = np.array(self.trips_df.loc[is_person, TRAVELDATE_COL])
        
        # Get minimum date against persons trips and the new host trip
        first_date = np.append(dates, kwargs['host_trip'][TRAVELDATE_COL]).max()
            
        return first_date
    
    def update_driver(self, **kwargs):
        """
        Update the driver for the new trip.
        If the host trip is a driver, then the new trip is a passenger, else then the new trip driver value is unchanged.
        
        1 = driver
        2 = passenger
        3 = both (switched drivers during trip)
        995 = na

        Returns:
            int/str: driver value
        """
        
        host_driver = kwargs['host_trip'][DRIVER_COL]
        
        if host_driver == 1:
            return 2
        else:
            return host_driver             
            
    def is_days_last(self, **kwargs):
        pass
    
    def is_days_first(self, **kwargs):
        pass
    

    