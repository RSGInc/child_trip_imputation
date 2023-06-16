from tqdm import tqdm
import pandas as pd
import numpy as np
from datetime import timedelta

# Internal imports
import settings
from nonproxy.populator import NonProxyTripPopulator
from nonproxy.timespace_buffer import fix_existing_joint_trips

# Constants
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES

PER_ID_NAME = settings.get_index_name('person')
TRIP_ID_NAME = settings.get_index_name('trip')
HH_ID_NAME = settings.get_index_name('household')
DAY_ID_NAME = settings.get_index_name('day')
HHMEMBER_PREFIX = COLNAMES['HHMEMBER']
DAYNUM_COL = COLNAMES['DAYNUM']
PNUM_COL = COLNAMES['PNUM']

assert isinstance(PER_ID_NAME, str), 'PER_ID_NAME not a string'
assert isinstance(TRIP_ID_NAME, str), 'TRIP_ID_NAME not a string'
assert isinstance(HH_ID_NAME, str), 'HH_ID_NAME not a string'


class ImputeNonProxyTrips:
    def joint_trip_member_table(self, persons_df: pd.DataFrame, trips_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        This function creates a table of joint trips from the trips table.
        A joint trip is defined as a trip that is reported as being made by more than one person. 
        The table is flattened so that each row is a joint trip member.
        Only trips that are reported as being a joint trip member, but who do not have a trip themselves are included.    

        Args:
            persons_df (pd.DataFrame): persons table
            trips_df (pd.DataFrame): trips table

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: A tuple of two dataframes:
                A joint trip member table containing the host trip ID, host person ID, joint trip member ID, and joint trip ID if available.
                and a similar dataframe but for non-joint trip trips.
        """    
            
        # Create member-trip table, replace any values > 1 with 0 (i.e., 995 missing values)    
        idx_cols = [settings.get_index_name(x) for x in ['person', 'household', 'day', 'trip']]
        idx_cols += ['joint_trip_num', DAYNUM_COL]
        
        trips = trips_df.reset_index().set_index(idx_cols)
        trips = trips.filter(regex=HHMEMBER_PREFIX).clip(0, 2).replace(2, 0)
        
        # Trip count checks
        n_trips = trips_df.shape[0]
        expected_n_trips = trips.sum().sum()
        deficit_n_trips = expected_n_trips - n_trips    
        
        print(f"Trips in trip table: {n_trips}")
        print(f"Expected trips as reported for household members (i.e., including joint trips): {expected_n_trips}")
        print(f"Joint trips to impute: {deficit_n_trips}")  
            
        # Drop non-joint trips
        member_count = trips.sum(axis=1)
        nonjoint_trip_ids = trips.loc[member_count == 1].index.get_level_values(TRIP_ID_NAME)    
        joint_trip_ids = trips.loc[member_count > 1].index.get_level_values(TRIP_ID_NAME)   
            
        # Flatten the table
        trips = pd.melt(
            trips.reset_index(),
            id_vars=trips.index.names,
            var_name=PNUM_COL,
            )
        
        # Drop empty member slots    
        trips = trips[trips.value == 1].drop(columns='value')
        
        # Get person number 
        trips.person_num = trips.person_num.str.replace(HHMEMBER_PREFIX, '').astype(int)
            
        # Create member ID table
        member_id = persons_df[[HH_ID_NAME, PNUM_COL]].reset_index().rename(columns={PER_ID_NAME: 'hh_member_id'})
        
        # Join person id onto member-trip table
        trips = trips.merge(member_id, on=[HH_ID_NAME, PNUM_COL], how='left')
        trips = trips.rename(columns={PNUM_COL: 'hh_member_num'})

        # Separate joint and non-joint trips    
        nonjoint_trips = trips[trips.trip_id.isin(nonjoint_trip_ids)]
        joint_trips = trips[trips.trip_id.isin(joint_trip_ids)]
        
        return joint_trips, nonjoint_trips

    def impute_nonproxy(self, persons_df, trips_df, **kwargs):    
            
        # Default parameters
        distance_threshold = kwargs.get('DISTANCE', 0.5)
        time_threshold = timedelta(minutes=kwargs.get('TIME', 30))

        # Create joint_trip_num column
        trips_df['joint_trip_num'] = pd.Series(0, index=trips_df.index, name='joint_trip_num')
        
        # 1. For each member-trip check if that person already has a trip but just wasn't reported as a joint trip member
        fixed_trips_df = fix_existing_joint_trips(trips_df, distance_threshold, time_threshold)
            
        # 2. Flatten and separate trip table into joint and non-joint trips
        joint_trips, nonjoint_trips = self.joint_trip_member_table(persons_df, fixed_trips_df)    
        
        # 4. Impute new joint-trips from reported but non-existent joint trips
        """
        Since we've already scanned and labeled the joint trips, we need to impute joint trips from the host trip.
        For each trip we create a new trip record, copying from the host trip and assigning a joint trip number
        """
        
        # Get unlabeled joint trips, for flexibility we allow for joint trips that are labeled as 0 or -1 or 995
        unlabeled_joint_trips = joint_trips[
            joint_trips.joint_trip_num.isna() | (joint_trips.joint_trip_num <= 0) | (joint_trips.joint_trip_num == 995)
            ]

        # Trim and sort index for performance
        unlabeled_joint_trips = unlabeled_joint_trips.drop(columns=['joint_trip_num', DAY_ID_NAME]).sort_values(TRIP_ID_NAME)
        
        # Initialize trip populator class to hold the data and manage trip counts
        Populator = NonProxyTripPopulator(persons_df, trips_df)
        
        new_trips_ls = []
        idx_names = [TRIP_ID_NAME, HH_ID_NAME, DAYNUM_COL]

        # For each household member trip that is not self reported, create a new joint trip
        print("Imputing missing proxy reported joint trips...")
        for host_idx, members in tqdm(unlabeled_joint_trips.groupby(idx_names)):
            host_trip_id, host_hh_id, host_daynum = host_idx
            
            # Get host trip and update the joint trip number
            host_trip = fixed_trips_df.loc[host_trip_id].copy()        
            host_trip['joint_trip_num'] = Populator.iterate_counter('joint_trip', host_hh_id, host_daynum)
            
            for i, hh_member_id, hh_member_num in members[['hh_member_id', 'hh_member_num']].itertuples():
                # Populate new trip            
                new_trip = Populator.populate(host_trip, hh_member_id, hh_member_num)
                new_trips_ls.append(new_trip)
        
        new_trips_df = pd.concat(new_trips_ls, axis=1, ignore_index=False).T
        new_trips_df.index.name = TRIP_ID_NAME
        
        assert len(set(new_trips_df.index).intersection(fixed_trips_df.index)) == 0, "New trips should not have the same index as existing trips"
        
        combined_trips_df = pd.concat([fixed_trips_df, new_trips_df])
        # combined_trips_df.loc[combined_trips_df.joint_trip_num == 0, 'joint_trip_num'] = 995
        
        return combined_trips_df
