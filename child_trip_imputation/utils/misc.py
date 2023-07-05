import numpy as np
import pandas as pd
import settings
from scipy.stats import gaussian_kde

# Constants
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES

JOINT_TRIPNUM_COL = COLNAMES['JOINT_TRIPNUM']
TRIPNUM_COL = COLNAMES['TRIPNUM']
HH_ID_NAME = COLNAMES['HH_ID_NAME']
PER_ID_NAME = COLNAMES['PER_ID_NAME']
OTIME = COLNAMES['OTIME']
DTIME = COLNAMES['DTIME']
OHOUR = COLNAMES['OHOUR']
DHOUR = COLNAMES['DHOUR']

# Codes
assert isinstance(settings.CODES, dict)
SCHOOL_PURPOSES_COL, SCHOOL_PURPOSES_CODES = settings.get_codes('SCHOOL_PURPOSES')

assert isinstance(PER_ID_NAME, str), 'PER_ID_NAME must be a string'
assert isinstance(TRIPNUM_COL, str), 'TRIPNUM_COL must be a string'
assert isinstance(HH_ID_NAME, str), 'HH_ID_NAME must be a string'
assert isinstance(JOINT_TRIPNUM_COL, str), 'JOINT_TRIPNUM_COL must be a string'
    
def cat_joint_trip_id(row: pd.Series) -> int:
    """
    Local function to generate a joint trip ID by concatenating the household ID and the joint trip number.

    Args:
        row (pd.Series): The trips table row

    Returns:
        int: the joint trip ID
    """
    
    hh_id = row[HH_ID_NAME]
    joint_trip_num = row[JOINT_TRIPNUM_COL]
    
    return int(f'{hh_id}{joint_trip_num:02d}')

def cat_trip_id(row: pd.Series) -> int:
    """
    Local function to generate a trip ID by concatenating the person ID and the joint trip number.

    Args:
        row (pd.Series): The trips table row

    Returns:
        int: the joint trip ID
    """    
    
    person_id = row[PER_ID_NAME]
    trip_num = row[TRIPNUM_COL]
    
    return int(f'{person_id}{trip_num:03d}')

# Generate a function that returns the disjoint set of a graph
def disjoint_set(edges: np.ndarray) -> dict:
    """
    This function returns the disjoint set of a graph of edges

    Args:
        edges (np.ndarray): The trip pairs to be checked for disjointedness

    Returns:
        dict: A dictionary of disjointed sets withere the key is the index ID of the trip and the value is the set ID (aka joint trip number)
    """
    
    parents = {}
    for i, edge in enumerate(edges, start=1):
        for x in edge:
            if x not in parents:
                parents[x] = i
    
    return parents

def get_dep_arr_dist(trips_df: pd.DataFrame, persons_df: pd.DataFrame, method: str) -> dict:
        """
        Aggregates trip times to the specified time increment.

        Args:
            trips_df (pd.DataFrame): The trips table.
            method (str): The method to use for aggregating times.
                Either 'bin' or 'kde' where:
                    'bin' is a discrete binning of time on specified time increment, and
                    'kde' is a Gaussian kernel density estimation of times in seconds.

        Returns:
            pd.DataFrame: The trips table with aggregated times.
        """
        
        timeinc = settings.TIME_INCREMENT
        assert method.lower() in ['bin', 'kde'], 'Method must be either "bin" or "kde"'
        assert isinstance(timeinc, str), 'Time increment must be a string'
        assert 'Min' in timeinc or 'H' in timeinc, 'Time increment must be in minutes (Min) or hours (H), e.g., 15Min or 1H'
        
                
        # Extract school trips
        school_trips = trips_df[trips_df[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES)]
        
        # Join the person type to determine school level
        # TODO
               
        
        # Convert to datetime in correct time zone
        dep_time = school_trips[OTIME].dt.tz_convert(settings.LOCAL_TIMEZONE)
        arr_time = school_trips[DTIME].dt.tz_convert(settings.LOCAL_TIMEZONE)
        dur_time = arr_time - dep_time
        
        # Confirm time zone is correct
        assert all(school_trips[OHOUR] == dep_time.dt.hour), 'Depart hour does not match timestamp, is time zone incorrect?'
        assert all(school_trips[DHOUR] == arr_time.dt.hour), 'Arrive hour does not match timestamp, is time zone incorrect?'
        
        # Bin by time increment            
        if method == 'bin':            
            dur_dist = (dur_time / pd.Timedelta('1 minute')).astype(int).value_counts().sort_index()
            dur_dist.index.name = 'minutes'
            
            # Combine to fill in NAs
            # deparr_dist = pd.concat({
            #     'depart': dep_time.dt.floor(timeinc).dt.time.value_counts().sort_index(),
            #     'arrive': arr_time.dt.floor(timeinc).dt.time.value_counts().sort_index()                
            #     }, axis=1).fillna(0).astype(int).rename_axis('time')        
            
            time_dist = {
                "depart": dep_time.dt.floor(timeinc).dt.time.value_counts().sort_index(),
                # "arrive": arr_time.dt.floor(timeinc).dt.time.value_counts().sort_index(),
                "duration_minutes": dur_dist
            }
        
        # Gaussian KDE interpolation
        else:            
            dep_seconds = ((dep_time - dep_time.dt.normalize()) / pd.Timedelta('1 second')).astype(int)
            dur_seconds = ((arr_time - dep_time) / pd.Timedelta('1 second')).astype(int)        

            # Max duration cutoff 1 hour
            dur_seconds = dur_seconds[dur_seconds < 60*60]
            
            # 60 * 60 * 24 = 86400 seconds in a day
            time_span = np.arange(start=1, stop=60*60*24, step=1)
            dur_span = np.arange(start=1, stop=dur_seconds.max(), step=1)
            dep_dens = gaussian_kde(dep_seconds).evaluate(time_span)
            dur_dens = gaussian_kde(dur_seconds).evaluate(dur_span)
        
            # import matplotlib.pyplot as plt
            # plt.plot(dep_dens)
            # plt.plot(dur_dens)
            # plt.show()
                        
            time_dist = {
                'depart_time': pd.Series(dep_dens, index=pd.to_datetime(time_span, unit='s').time, name='depart_time'),
                'duration_seconds': pd.Series(dur_dens, index=pd.to_timedelta(dur_span, unit='s'), name='duration_seconds')
            }
        
                
        return time_dist