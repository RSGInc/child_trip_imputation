import pandas as pd
import numpy as np
import settings
from utils.misc import cat_joint_trip_id, cat_trip_id

# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'

COLNAMES = settings.COLUMN_NAMES
PER_ID_NAME = COLNAMES['PER_ID_NAME']
TRIP_ID_NAME = COLNAMES['TRIP_ID_NAME']
HH_ID_NAME = COLNAMES['HH_ID_NAME']
TRIPNUM_COL = COLNAMES['TRIPNUM']
JOINT_TRIP_ID_NAME = COLNAMES['JOINT_TRIP_ID_NAME']
JOINT_TRIPNUM_COL = COLNAMES['JOINT_TRIPNUM']


class TripCounter:
    """
    This trip counter class is intended to be a global helper class to keep track 
    of the latest trip number and trip ID for each person.
    """
    
    def __init__(self, trips_df: pd.DataFrame|None = None, person_df: pd.DataFrame|None = None) -> None:        
        if trips_df:
            self.initialize(trips_df, person_df)
    
    def initialize(self, trips_df: pd.DataFrame, person_df: pd.DataFrame|None) -> None:
        """
        Initialize the trip counter and joint trip counter DataFrames.
        Can be used to reset the trip counter if needed.

        Args:
            trips_df (pd.DataFrame): The trips table
            
        Rturns: None but sets the trip_counter and joint_trip_counter attributes to the class object.
        """
        assert isinstance(trips_df, pd.DataFrame), 'trips_df must be a DataFrame'        
        
        # Group by person id and household id
        trip_grp = trips_df.reset_index().groupby([PER_ID_NAME])
        joint_trip_grp = trips_df.reset_index().groupby([HH_ID_NAME])    
        
        # Get the max trip number and trip ID for each person
        self.trip = trip_grp.aggregate({TRIPNUM_COL: 'max', TRIP_ID_NAME: 'max'})
        self.joint_trip = joint_trip_grp.aggregate({JOINT_TRIPNUM_COL: 'max', JOINT_TRIP_ID_NAME: 'max'})
        
        # # Insert 0 trip number for persons with 0 trips
        # self.trip = self.trip.reindex(person_df.index, fill_value=0)
        # self.joint_trip = self.joint_trip.reindex(person_df.index, fill_value=0)        
          
        # zero_trips = self.trip[self.trip.trip_id == 0].index
        # zero_joint_trips = self.joint_trip[self.joint_trip.joint_trip_id == 0].index
        
        # self.trip[zero_trips] = self.trip[zero_trips].reset_index().apply(cat_trip_id, axis=1)
        # self.joint_trip[zero_joint_trips] = self.joint_trip[zero_joint_trips].reset_index().apply(cat_joint_trip_id, axis=1)
        
        return        
    
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
        counter_df = getattr(self, counter_name)
                
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
        assert hasattr(self, counter_name), f'counter_name {counter_name} not an attribute of TripCounter'
        setattr(self, counter_name, counter_df)
                                  
        return count

# Initialize the global trip counter object
TRIP_COUNTER = TripCounter()