import os 
import random
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

import settings
from school_trips.base_manager import ManagerClass
from school_trips.household import HouseholdManagerClass
from school_trips.day import DayManagerClass
from school_trips.tour import TourManagerClass

from sklearn.metrics.pairwise import haversine_distances
from utils.misc import get_dep_arr_dist
from utils.io import DBIO
from utils.trip_counter import TRIP_COUNTER


# CONSTANTS
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
assert isinstance(settings.IMPUTATION_CONFIGS, dict), 'IMPUTATION_CONFIGS not a dict'

COLNAMES = settings.COLUMN_NAMES
COL_ACTIONS_PATH = settings.IMPUTATION_CONFIGS.get('impute_school_trips')
SCHOOL_PURPOSES_COL, SCHOOL_PURPOSES_CODES = settings.get_codes(('SCHOOL_PURPOSES', 'PURPOSE'))
SCHOOL_PURPCAT_COL, SCHOOL_PURPCAT_CODES = settings.get_codes(('SCHOOL_PURPOSES', 'PURPOSE_CATEGORY'))
IMPUTED_SCHOOL_PURPOSE_CAT = settings.IMPUTED_SCHOOL_PURPOSE_CAT

DLAT, DLON = COLNAMES['DLAT'], COLNAMES['DLON']
OLAT, OLON = COLNAMES['OLAT'], COLNAMES['OLON']
HOMELAT, HOMELON = COLNAMES['HOMELAT'], COLNAMES['HOMELON']
DAY_ID_NAME = COLNAMES['DAY_ID']
PER_ID_NAME = COLNAMES['PER_ID']
TRIP_ID_NAME = COLNAMES['TRIP_ID']
AGE_COL = COLNAMES['AGE']
TRIPNUM_COL = COLNAMES['TRIPNUM']
OTIME = COLNAMES['OTIME']

SCHOOL_PURPOSE_AGE = settings.SCHOOL_PURPOSE_AGE
IMPUTED_PURPCAT = settings.IMPUTED_SCHOOL_PURPOSE_CAT

# Aggregate trip times for school trips for sampling
TRIPS_DF = DBIO.get_table('trip')
PERSON_DF = DBIO.get_table('person')
assert isinstance(TRIPS_DF, pd.DataFrame), 'TRIPS_DF must be a DataFrame'
assert isinstance(PERSON_DF, pd.DataFrame), 'PERSON_DF must be a DataFrame'

DEP_FREQ, DUR_FREQ = get_dep_arr_dist(TRIPS_DF, PERSON_DF, method='KDE').values()

# Initialize static objects once at the module level to avoid re-initializing them in each time the class is instantiated
assert isinstance(COL_ACTIONS_PATH, str), 'COL_ACTIONS_PATH must be a string'
assert os.path.isfile(COL_ACTIONS_PATH), f'File {COL_ACTIONS_PATH} does not exist'

# Read in actions config table
ACTIONS = pd.read_csv(COL_ACTIONS_PATH)

# TIMEZONES
LOCAL_TZ = settings.LOCAL_TIMEZONE
DATA_TZ = str(TRIPS_DF[OTIME].dt.tz)
        
class TripManagerClass(ManagerClass):
    # Tour manager is not fully implemented yet, so optional for now
    def __init__(self, trip: pd.DataFrame|None, Day: DayManagerClass, Tour: TourManagerClass|None) -> None:        
        super().__init__(trip)
        self.Tour = Tour
        self.Day = Day
    
    def impute_from_escort(self, escort_trip: pd.DataFrame) -> pd.DataFrame:
        """ 
        - Use characteristics of household member escorting trip: mode, time, number of participants, etc.
        - Use this as the time matching criteria for an allowed time difference to try to match joint tours together.
        - Stop location should be within X miles of school location. (threshold should be a setting)
        """
        # TODO: Implement
        new_trip = pd.DataFrame()
        
        return new_trip
    
    def impute_from_altday(self, altday_trip: pd.DataFrame) -> pd.DataFrame:
        """
        - Use characteristics of other school trip. If multiple, just take first instance
        - Only applies to children age 18+ that are using rMove.
        """
        
        # TODO: Implement
        new_trip = pd.DataFrame()
        
        return new_trip
    
    def impute_new_school_trip(self) -> pd.Series:
        # TODO: Implement        
        """
        - Sample departure and arrival time
        - Use the reported typical mode to school unless the child has a school trip in the opposite direction, 
        in which case that mode should be used.
        
        - The child is not considered to be escorted by another household member (otherwise they would have been
        captured by the first check looking at whether another household member escorted them), but if the mode 
        selected is vehicle and the child is under the age of 16, assume the child is riding in car pool with at 
        least one other student and one adult, leading to the occupancy level of shared ride 3+ 
        
        - Requires a valid school location to create school trip. (Could relax this condition by integrating landuse 
        file and finding the nearest zone with enrollment matching the school type for the child. There are only 3 
        children in elementary, middle, or high school without a valid school location, so adding this functionality
        at the start is likely not worth the effort.)
        
        - Need to create a return trip home using the same mode at the end of the school day if one does not already exist.
        """
        
        assert isinstance(self.Day.Person.data, pd.Series), 'Day.Person.data must be a Series'
        
        # # Initialize new dummy "host" trip on that day for values to replace
        # new_trip = self.Day.get_related('trip', on=['day_num', 'person_id'])
        # # If no trip on that day, expand to all days and set day num to current day
        # if new_trip.empty:
        #     new_trip = self.Day.get_related('trip', on=['person_id'])               
             
        # assert not new_trip.empty, 'No trip on day'
        assert isinstance(self.Day.data, pd.Series), 'Day.data must be a Series'
        assert isinstance(DAY_ID_NAME, str), 'DAY_ID_NAME must be a string'
        
        new_trip = pd.Series()
        # new_trip = new_trip.iloc[0].copy()
        new_trip['day_num'] = self.Day.data['day_num']        
        new_trip[DAY_ID_NAME]  = self.Day.data[DAY_ID_NAME]
            
        # Reserved columns - meaning they are not checked for in the trip_column_actions.csv file        
        reserved_cols = [COLNAMES['JOINT_TRIPNUM'], COLNAMES['JOINT_TRIP_ID'], 'tour_id', 'tour_num', 'tour_type']
        actions_dict = {i: df.colname for i, df in ACTIONS.groupby('impute_new_school_trip')}
        
        # Populate new trip
        self.populate(host_record=new_trip, table='trip', reserved_cols=reserved_cols, actions=actions_dict)        
        
        return new_trip
    
    def sample_times(self, **kwargs) -> dict:
        
        fields = kwargs.get('fields')
        
        assert isinstance(fields, list), 'Fields must be a list'
        assert isinstance(self.Day.data, pd.Series), 'Day.data must be a DataFrame'
        assert isinstance(LOCAL_TZ, str), 'TIME_ZONE must be a string'

        # Make random choice for departure and duration times
        depart_time = random.choices(DEP_FREQ.index, weights=DEP_FREQ, k=1)[0]
        duration_time = random.choices(DUR_FREQ.index, weights=DUR_FREQ, k=1)[0]
        
        # Add date component
        depart_date = self.Day.data['travel_date']
        depart_time = datetime.combine(depart_date, depart_time)
        
        # Set local timezone, then convert to UTC to match the rest of the data
        depart_time = depart_time.replace(tzinfo=pytz.timezone(LOCAL_TZ))
        depart_time = depart_time.astimezone(pytz.timezone(DATA_TZ))
        
        #set_timezone(timezone)        
        arrive_time = depart_time + duration_time        
                
        # Create dictionary of results
        result = {
            'depart_time': depart_time,
            'arrive_time': arrive_time,
            'depart_date': depart_date,
            'arrive_date': depart_date,
            'depart_hour': depart_time.hour,
            'depart_minute': depart_time.minute,
            'depart_seconds': depart_time.second,
            'arrive_hour': arrive_time.hour,
            'arrive_minute': arrive_time.minute,
            'arrive_second': arrive_time.second,
            'duration_minutes': duration_time.seconds / 60,
            'duration_seconds': duration_time.seconds,                        
            }
        
        assert set(fields) - set(result.keys()) == set(), f'Missing fields: {set(fields) - set(result.keys())}'
                
        return result
            
    def copy_from(self, table: str, **kwargs) -> dict:
        """
        Generic method to copy fields from a table to the new record.

        Args:
            table (str): The table to copy from. Either day, person, household, else it is assumed to be host trip.

        Returns:
            pd.Series: The values to copy to the new record.
        """
        assert table in ['day', 'person', 'household', 'host'], 'Table must be day, person, household, or host'
        
        fields = kwargs.get('fields', [])
        from_field = kwargs.get('from_field', None)
        
        if table == 'day':
            host = self.Day.data
        elif table == 'person':
            host = self.Day.Person.data
        elif table == 'household':
            host = self.Day.Person.Household.data
        else:
            host = kwargs.get('host_record')
        
        if isinstance(host, pd.DataFrame):
            host = host.reset_index().iloc[0]
        
        assert isinstance(fields, list), 'Fields must be a list'                
        assert isinstance(host, pd.Series), f'{table} record must be a DataFrame or Series'
        
        if from_field is not None:            
            # Copy each from_field to the new field
            for field in fields:
                host[field] = host[from_field]            
        
        assert set(fields) - set(host.index) == set(), f'Fields {set(fields) - set(host.index)} must be in host_record'
        
        return host[fields].to_dict()
    
    def copy_from_day(self, **kwargs) -> dict:
        return self.copy_from(table='day', **kwargs)
        
    def copy_from_person(self, **kwargs) -> dict:
        return self.copy_from(table='person', **kwargs)
    
    def copy_from_household(self, **kwargs) -> dict:
        return self.copy_from(table='household', **kwargs)
    
    def copy_from_host(self, **kwargs) -> dict:
        return self.copy_from(table='host', **kwargs)
        
    def generate_trip_id(self, **kwargs) -> dict:             
        assert isinstance(self.Day.Person.data, pd.Series), 'Day.Person.data must be a Series'         
        assert isinstance(PER_ID_NAME, str), 'PER_ID_NAME must be a string'
        assert isinstance(TRIPNUM_COL, str), 'TRIPNUM_COL must be a string'
        
        person_id = int(self.Day.Person.data[PER_ID_NAME])     
        
        return {TRIP_ID_NAME: TRIP_COUNTER.trip.loc[person_id, TRIPNUM_COL]}
    
    def update_trip_num(self, **kwargs) -> dict: 
        """
        Update the trip number for the new trip.

        Returns:
            int|str: returns the new trip number
        """
        assert isinstance(self.Day.Person.data, pd.Series), 'Day.data must be a DataFrame'
        assert isinstance(PER_ID_NAME, str), 'PER_ID_NAME must be a string'
        assert isinstance(TRIPNUM_COL, str), 'TRIPNUM_COL must be a string'
        assert isinstance(TRIP_ID_NAME, str), 'TRIP_ID_NAME must be a string'
        
        person_id = int(self.Day.Person.data[PER_ID_NAME])
        assert isinstance(person_id, (int, str, np.integer)), 'Person ID must be an integer'
        
        trip_num = TRIP_COUNTER.iterate_counter('trip', person_id)
        trip_id = TRIP_COUNTER.trip.loc[person_id, TRIP_ID_NAME]
        
        return {TRIPNUM_COL: trip_num, TRIP_ID_NAME: trip_id}
    
    def get_school_location(self, **kwargs) -> dict:

        assert isinstance(DLAT, str) and isinstance(DLON, str), 'DLAT and DLON must be a string'
        assert isinstance(OLAT, str) and isinstance(OLON, str), 'OLAT and OLON must be a string'
        assert isinstance(HOMELAT, str) and isinstance(HOMELON, str), 'HOMELAT and HOMELON must be a string'
     
        fields = kwargs.get('fields')
        assert isinstance(fields, list), 'Fields must be a list'
                
        # If they have a school purpose in another trip, use that
        trips = self.Day.Person.get_related('trip')
        school_trips = trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES).values

        # If they have an existing school location in another trip, use that
        if school_trips.any():
            return trips.loc[school_trips, fields].iloc[0].to_dict()
        
        # Otherwise find nearest school location of the same type
        trips_df =  DBIO.get_table('trip')
        assert isinstance(trips_df, pd.DataFrame), 'Trips must be a DataFrame'
        assert isinstance(self.Day.Person.Household.data, pd.Series), 'Class manager household must be a DataFrame'
        
        # Find persons with same school type, then get their trips
        person_shared_schools = self.Day.Person.get_related('person', 'school_type')        
        trip_shared_schools = trips_df[trips_df[PER_ID_NAME].isin(person_shared_schools.index)]        
               
        # Convert lat/lon to radians for pairwise haversine distance calculation
        olatlons = self.Day.Person.Household.data[[HOMELAT, HOMELON]].astype(float).to_numpy()
        olatlons = np.radians(np.expand_dims(olatlons, axis=0))            
        dlatlons = np.radians(trip_shared_schools[[DLAT, DLON]].to_numpy())
        
        # Find distances between home and known school locations
        dist = haversine_distances(olatlons, dlatlons)*settings.R
        
        assert isinstance(settings.MAX_SCHOOL_DIST, (int, float)), 'MAX_SCHOOL_DIST must be a number'
        # Take the minimum distance, if any meet criteria        
        if np.any(dist < settings.MAX_SCHOOL_DIST):
            return trip_shared_schools.iloc[np.argmin(dist)][fields].to_dict()
               
        # Otherwise return nan
        return {field: np.nan for field in fields}
        
    def get_purpose(self, **kwargs) -> dict:
        
        assert isinstance(self.Day.Person.data, pd.Series), 'Class manager person must be a DataFrame'
        assert isinstance(AGE_COL, str), 'AGE_COL must be a string'
        assert isinstance(SCHOOL_PURPOSES_COL, str), 'SCHOOL_PURPOSES_COL must be a string'
        
        # If they have a school purpose in another trip, use that
        trips = self.Day.Person.get_related('trip')
        # school_trips = trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES)        
        school_trips = trips[SCHOOL_PURPCAT_COL].isin(SCHOOL_PURPCAT_CODES)      
                
        if school_trips.any():
            purpose = trips.loc[school_trips, SCHOOL_PURPOSES_COL]            
            return {SCHOOL_PURPOSES_COL: purpose, SCHOOL_PURPCAT_COL: IMPUTED_SCHOOL_PURPOSE_CAT}
                    
        # Otherwise get purpose by age, loop thru the purpose type by age and return the first match
        assert isinstance(SCHOOL_PURPOSE_AGE, dict), 'SCHOOL_PURPOSE_AGE must be a dictionary'
        for purpose, ages in SCHOOL_PURPOSE_AGE.items():
            assert isinstance(ages, list), 'Ages must be a list'
            if self.Day.Person.data[AGE_COL] in ages:
                return {SCHOOL_PURPOSES_COL: purpose, SCHOOL_PURPCAT_COL: IMPUTED_SCHOOL_PURPOSE_CAT}

                
        # All else return nan        
        return {SCHOOL_PURPOSES_COL: np.nan, SCHOOL_PURPCAT_COL: IMPUTED_SCHOOL_PURPOSE_CAT}
             
    def update_first_date(self, **kwargs) -> dict:
        """
        Update the first date for the new trip. Takes the minimum date from the member's trips and the host trip.

        Returns:
            datetime object: The first trip date
        """
        return {COLNAMES['TRAVELDATE']: self.Day.Person.get_related('trip')[COLNAMES['TRAVELDATE']].min()}
    
    def update_last_date(self, **kwargs) -> dict:
        """
        Update the last date for the new trip. Takes the maximum date from the member's trips and the host trip.

        Returns:
            datetime object: The last trip date
        """
        return {COLNAMES['TRAVELDATE']: self.Day.Person.get_related('trip')[COLNAMES['TRAVELDATE']].max()}
