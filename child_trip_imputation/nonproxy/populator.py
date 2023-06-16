import re
import pandas as pd
import numpy as np

import settings


# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES

PER_ID_NAME = settings.get_index_name('person')
TRIP_ID_NAME = settings.get_index_name('trip')
HH_ID_NAME = settings.get_index_name('household')
DAYNUM_COL = COLNAMES['DAYNUM']
TRAVELDATE_COL = COLNAMES['TRAVELDATE']
DRIVER_COL = COLNAMES['DRIVER']

class NonProxyTripPopulator:
    """
    Create a new trip from a host trip and a non-proxy household member, 
    the attributes are populated using the defined class methods listed in the trip_column_actions.csv file
    """
    
    def __init__(self, person_df: pd.DataFrame, trips_df: pd.DataFrame) -> None:
        self.actions = pd.read_csv('trip_column_actions.csv')
        self.trips_df = trips_df
        self.person_df = person_df
        
        # Trip counters
        self.trip_counter = trips_df.groupby([PER_ID_NAME, DAYNUM_COL]).trip_num.max()
        self.joint_trip_counter = trips_df.groupby([HH_ID_NAME, DAYNUM_COL]).joint_trip_num.max()        

    def populate(self, host_trip: pd.DataFrame, member_id: int|str, member_num: int|str) -> pd.DataFrame:         
        
        # Copy host trip and update column value
        new_trip = host_trip.copy()
        
        # Reserved columns - meaning they are not checked for in the trip_column_actions.csv file
        reserved_cols = ['joint_trip_num', 'joint_trip_id', 'tour_id', 'tour_num', 'tour_type']
        
        missing = set(new_trip.index).difference(set(self.actions.colname.to_list() + reserved_cols))
        assert len(missing) == 0, f'Columns {missing} not in trip_column_actions.csv'        

        # Loops through each column and applies the method to the host trip        
        for i, colname, method in self.actions.itertuples():
            # Local data to pass around
            locals_dict = {'colname': colname, 'host_trip': host_trip, 'member_id': member_id, 'member_num': member_num}            
            
            # Check if the method has a direct set value argument
            value = self.set_value(method)
            
            # otherwise call the method
            if value is None:
                assert hasattr(self, method), f'No method {method} found'
                value = getattr(self, method)(**locals_dict)

            # Defaults to None
            new_trip[colname] = value
            
        # Update the trip id
        new_trip_id = int(f'{new_trip.person_id}{new_trip.trip_num:03d}')
        new_trip['joint_trip_id'] = int(f'{new_trip.hh_id}{new_trip.joint_trip_num:02d}')
        
        
        assert new_trip_id not in self.trips_df.index, f'Trip id {new_trip_id} already exists'
        new_trip.name = new_trip_id
        
        return new_trip
    
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
    
    def iterate_counter(self, num_item: str, item_id: int|str, host_daynum: int|str) -> int:        
        """
        Find the current max trip number and iterate.
        
        Args:
            num_item (str): ['joint_trip', 'trip'] The name of the trip number column to iterate
            _id (int|str): The household id for the joint_trip_counter or the household member person id
            host_daynum (int|str): The host trip day number

        Returns:
            int: The new trip number
        """
        
        assert num_item in ['joint_trip', 'trip'], f'num_item must be one of {["joint_trip", "trip"]}'
        
        if num_item == 'joint_trip':
            assert len(str(item_id)) == 8, 'joint trip household id must be 10 digits, is this the wrong id?'
        else:
            assert len(str(item_id)) == 10, 'trip person id must be 10 digits, is this the wrong id?'
                
        # Retrieve the trip counter        
        counter_df = getattr(self, num_item + '_counter')
        
        # Initialize the counter if it doesn't exist
        if (item_id, host_daynum) not in counter_df.index:
            newindex = pd.MultiIndex.from_tuples([(item_id, host_daynum)], names=[num_item + '_id', DAYNUM_COL])
            newindex = counter_df.index.append(newindex)
            counter_df = counter_df.reindex(newindex, fill_value=0)
            
        # Iterate
        num = counter_df.loc[(item_id, host_daynum)]
        num = 1 if (np.isnan(num) or num == 995 or num < 0) else num + 1
        counter_df.loc[(item_id, host_daynum)] = num
        
        # Return the new trip number
        setattr(self, num_item, counter_df)
                                  
        return num
    
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
        day_num = kwargs['host_trip'][DAYNUM_COL]
        
        return self.iterate_counter('trip', member_id, day_num)
    
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
    

    