from tqdm import tqdm
import numpy as np
import pandas as pd
import pandas.api.types as ptypes

from sklearn.metrics.pairwise import haversine_distances
from datetime import timedelta

# Internal imports
import settings
from utils.misc import disjoint_set

# Constants
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES

PER_ID_NAME = COLNAMES['PER_ID']
TRIP_ID_NAME = COLNAMES['TRIP_ID']
HH_ID_NAME = COLNAMES['HH_ID']
OLAT, OLON = COLNAMES['OLAT'], COLNAMES['OLON']
DLAT, DLON = COLNAMES['DLAT'], COLNAMES['DLON']
HHMEMBER_PREFIX = COLNAMES['HHMEMBER']
DTIME_COL = COLNAMES['DTIME']
OTIME_COL = COLNAMES['OTIME']
DAYNUM_COL = COLNAMES['DAYNUM']
PNUM_COL = COLNAMES['PNUM']
MODE = COLNAMES['MODE']
JOINT_TRIP_ID_NAME = COLNAMES['JOINT_TRIP_ID']
JOINT_TRIPNUM_COL = COLNAMES['JOINT_TRIPNUM']

# This is ugly but passes the type checking
assert isinstance(PER_ID_NAME, str), 'PER_ID_NAME not a string'
assert isinstance(TRIP_ID_NAME, str), 'TRIP_ID_NAME not a string'
assert isinstance(HH_ID_NAME, str), 'HH_ID_NAME not a string'
assert isinstance(DTIME_COL, str), 'DTIME_COL not a string'
assert isinstance(OTIME_COL, str), 'OTIME_COL not a string'
assert isinstance(OLAT, str), 'OLAT not a string'
assert isinstance(OLON, str), 'OLON not a string'
assert isinstance(DLAT, str), 'DLAT not a string'
assert isinstance(DLON, str), 'DLON not a string'
assert isinstance(MODE, str), 'MODE not a string'

def fix_existing_joint_trips(trips_df: pd.DataFrame, distance_threshold: float, time_threshold: timedelta) -> pd.DataFrame:
    """
    This function finds and fixes unreported joint trips. 
    This is done by checking each trip against all trips within the household using a time/distance threshold buffer
    around the OD location/time and checking if the member-trip is within that buffer.
    If the person-trip is within the buffer, the associated person is added as a joint-trip hh_member_#

    Args:
        trips_df (pd.DataFrame): Trips dataframe
        distance_threshold (float): Maximum buffer distance in feet
        time_threshold (timedelta): Maximum time buffer in minutes

    Returns:
        pd.DataFrame: fixed trips dataframe
    """
        
    # Setup the dataframe for faster processing    
    trim_cols = [PER_ID_NAME, HH_ID_NAME]
    trim_cols += [OLAT, OLON, DLAT, DLON] 
    trim_cols += [OTIME_COL, DTIME_COL, PNUM_COL, DAYNUM_COL, MODE, JOINT_TRIPNUM_COL]    
    # Get non-empty household member columns    
    # trim_cols += trips_df.filter(regex=HHMEMBER_PREFIX).columns.tolist()
    trim_cols += (trips_df.filter(regex=HHMEMBER_PREFIX).clip(0, 2).replace(2, 0).sum(axis=0) > 0).index.tolist()
    trim_cols += ['corrected_hh_members']
    
    # Pre-index on household_id and day_num for faster lookup
    trips_df['corrected_hh_members'] = 0    
    
    hh_trips_df = trips_df[trim_cols].reset_index().set_index([HH_ID_NAME, DAYNUM_COL]).sort_index()
    
    print('Finding unreported joint trips...')
    # Run loop in list comprehension for faster processing
    fixed_ls = [find_joint_hh_trips(hh_trips, distance_threshold, time_threshold) for hh_id, hh_trips in tqdm(hh_trips_df.groupby(level=(0, 1)))]
    
    # Drop empty frames
    fixed_ls = [df for df in fixed_ls if not df.empty]
    
    # Concatenate all the fixed joint trips into dataframe    
    fixed_joint_trips = pd.concat(fixed_ls).set_index(TRIP_ID_NAME).filter(regex=f'{HHMEMBER_PREFIX}|{JOINT_TRIPNUM_COL}|corrected_hh_members')
    
    # Store for debugging
    # trips_df_old = trips_df.copy()
    
    # Update the trips table with the fixed joint trips
    msg = f'Found {fixed_joint_trips.shape[0]} joint trips and fixed '
    msg += f'{fixed_joint_trips.corrected_hh_members.sum()} unreported household members on the joint trips.'
    print(msg)
    trips_df.loc[fixed_joint_trips.index, fixed_joint_trips.columns] = fixed_joint_trips
    
    return trips_df

def find_joint_hh_trips(hh_trips: pd.DataFrame, distance_threshold: float, time_threshold: timedelta) -> pd.DataFrame:
    """
    This function finds and fixes unreported household members on joint trips
    and also assigns a joint trip number label to the joint trip.
    It returns a corrected dataframe, or an empty dataframe.

    Args:
        hh_trips (pd.DataFrame): The household trips dataframe

    Returns:
        dict: Dict with trip ids as keys and a dict with their respective distance/time distance as values.
    """
    
    # Convert lat/lon to radians for pairwise haversine distance calculation
    olatlons = np.radians(hh_trips[[OLAT, OLON]].to_numpy())
    dlatlons = np.radians(hh_trips[[DLAT, DLON]].to_numpy())
    otimes = hh_trips.depart_time.to_numpy()
    dtimes = hh_trips.arrive_time.to_numpy()
    
    # Calculate time difference between origins and destinations
    otimedelta = np.abs(otimes[np.newaxis,:] - otimes[:,np.newaxis])
    dtimedelta = np.abs(dtimes[np.newaxis,:] - dtimes[:,np.newaxis])
    
    # Find distance between origin to origin and destination to destination
    odist = haversine_distances(olatlons, olatlons)*settings.R
    ddist = haversine_distances(dlatlons, dlatlons)*settings.R
    
    # Find where distances are below the threshold
    distmat = (odist < distance_threshold) * (ddist < distance_threshold)
    timemat = (otimedelta < time_threshold) * (dtimedelta < time_threshold)
    
    # Combined distance and time matrices
    threshold_matrix = distmat * timemat
    np.fill_diagonal(threshold_matrix, False)
    
    # Get the indices and tranpose to get a list of paired indices
    threshold_idx = np.where(threshold_matrix)
    threshold_idx = np.array(threshold_idx).transpose()
    
    # Initialize empty arrays        
    joint_idx = np.ndarray((0,2), dtype=int)    
    joint_trip_nums, idx = {}, []
    
    # Iterate through the paired indices and check if they are unreported joint trips
    # We check both pairs A->B and B->A to ensure we get unreported joint trips for both parties
    for a_row, b_row in threshold_idx:
        # Check if same mode was used
        is_shared_mode = hh_trips.iloc[a_row][MODE] == hh_trips.iloc[b_row][MODE]        
        
        # Check if person B is unreported in the corresponding trip A
        member_b = f"{HHMEMBER_PREFIX}{hh_trips.iloc[b_row].person_num}"
        is_unreported = hh_trips.iloc[a_row][member_b] == 0
        
        # If it is a joint trip, add to the new arrays so it create joint trip label later
        if is_shared_mode:
            joint_idx = np.append(joint_idx, [[a_row, b_row]], axis=0)
            
        # TODO match or correct purpose (e.g., escort to school?)
        
        # Correct the unreported joint trip hh member
        if is_shared_mode and is_unreported:            
            a_col = hh_trips.columns.get_loc(member_b)            
            c_col = hh_trips.columns.get_loc('corrected_hh_members')
            
            hh_trips.iloc[a_row, a_col] = 1                              
            
            # Doing this weird pass off to avoid type errors
            val = hh_trips.iloc[a_row, c_col]
            assert isinstance(val, int) or type(val) == np.int64, f'Expected int, got {type(val)}'                      
            val += 1            
            hh_trips.iloc[a_row, c_col] = val
            

    if joint_idx.size > 0:
        # Assign a unique joint trip number using some graph theory to find the connected person-trips
        joint_trip_nums = disjoint_set(joint_idx)
        
        # Retrieve the column and row indices for the joint trip    
        jt_col = hh_trips.columns.get_loc(JOINT_TRIPNUM_COL)
        idx = list(joint_trip_nums.keys())
        
        # Set the joint trip number    
        hh_trips.iloc[idx, jt_col] = pd.Series(joint_trip_nums.values())
    
    return hh_trips.iloc[idx]

    # Construct result dictionary   
    # result_id = dict(zip(hh_trips.index.names, hh_trips.index.values[0]))
    # idx = (idx_a, idx_b)        
    # # Return the trip IDs pairs        
    # calculations = {
    #     'trip_id_a': hh_trips.iloc[idx_a].trip_id.values,
    #     'trip_id_b': hh_trips.iloc[idx_b].trip_id.values,
    #     'odist': odist[idx],
    #     'ddist': ddist[idx], 
    #     'otimedelta': otimedelta[idx], 
    #     'dtimedelta': dtimedelta[idx]
    #     }
    # result = pd.DataFrame({**result_id, **calculations})  
