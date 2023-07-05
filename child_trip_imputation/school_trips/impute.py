from tqdm import tqdm
import pandas as pd

# Internal imports
import settings
from utils.io import DBIO
from utils.trip_counter import TRIP_COUNTER
from school_trips.household import HouseholdManagerClass
from school_trips.day import DayManagerClass
from school_trips.person import PersonManagerClass
from school_trips.trip import TripManagerClass

# CONSTANTS
assert isinstance(settings.CODES, dict) 
CHILD_AGE_COL, CHILD_AGE_CODES = settings.get_codes('CHILD_AGE')
PRESCHOOL_AGE_COL, PRESCHOOL_AGE_CODES = settings.get_codes('PRESCHOOL_AGE')
PRESCHOOL_TYPE_COL, PRESCHOOL_TYPE_CODES = settings.get_codes('PRESCHOOL_TYPES')
SCHOOL_PURPOSES_COL, SCHOOL_PURPOSES_CODES = settings.get_codes('SCHOOL_PURPOSES')
ESCORT_PURPOSE_COL, ESCORT_PURPOSE_CODES = settings.get_codes('ESCORT_PURPOSES')


class ImputeSchoolTrips:
    new_trips = []
    
    # def __init__(self, trips_df: pd.DataFrame) -> None:
        # Initialize TripCounter at this outer class level to avoid initializing within-loops
        # TripCounter.__init__(self, trips_df)
        
    
    def impute_school_trips(self) -> None:        
        
        households_df = DBIO.get_table('household')
        # persons_df = DBIO.get_table('person')
        trips_df = DBIO.get_table('trip')
        
        assert isinstance(households_df, pd.DataFrame), 'household table is not a DataFrame'
        assert isinstance(trips_df, pd.DataFrame), 'trip table is not a DataFrame'
        # assert isinstance(persons_df, pd.DataFrame), 'person table is not a DataFrame'        
        
        # Initialize trip counter with latest trips table
        TRIP_COUNTER.initialize(trips_df)
        
        # Level 1 outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(households_df.groupby(level=0)):
            Household = HouseholdManagerClass(hh)            
            # Level 2 loop over persons in household, adding Household to Person manager
            for person_id, person in Household.get_related('person').groupby(level=0):
                Person = PersonManagerClass(person, Household)                            
                # Level 3 loop over days for persons, adding Person to Day manager
                for day_id, day in Person.get_related('day').groupby(level=0):
                    Day = DayManagerClass(day, Person)                    
                    # Skip if imputation is not required for this person-day
                    if self.is_missing_school_trip(Day, person):
                        # Impute missing school trips
                        self.school_trip_imputation(Day)
                
                    # # Level 4 loop over tours to populate tours
                    # for tour_id, tour in Day.get_related('tour').groupby(level=0):
                    #     # There is no tour table yet, create an empty dummy to be populated
                    #     Tour = TourManagerClass(tour, Day)
                    #     person_day_tour_trips = Tour.get_related('trip', on=['hh_id', 'day_num', 'tour_num'])
                    #     Tour.populate_tour(person_day_tour_trips)
                    
        print('Done')
                        
    def school_trip_imputation(self, Day: DayManagerClass) -> None:
        # Initialize a new trip manager for the child
        Trip = TripManagerClass(trip=None, Day=Day, Tour=None)
        
        # 1) Does any other household member report escorting trip with student?
        # Includes trips from all other hh members on that day
        hh_day_trips = Day.get_related('trip', on=['hh_id', 'day_num'])        
        escort_trips = hh_day_trips[ESCORT_PURPOSE_COL].isin(ESCORT_PURPOSE_CODES)
        if escort_trips.any():
            # Loop over escort trips, if any, and create a new trip for the child
            # This might have already been taken care of in the "nonproxy" step above                        
            for escort_trip in hh_day_trips[escort_trips].itertuples():                
                self.new_trips.append(
                    Trip.impute_from_escort(escort_trip)
                    )

            return
        
        # 2) Else if there a school trip on another day for that child?
        # Includes trips from all other days for that person
        person_trips = Day.Person.get_related('trip', on = ['hh_id', 'person_num'])        
        altday_trips = person_trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES)
        
        assert isinstance(Day.Person.data, pd.DataFrame), f'Class data not a DataFrame, expecting DataFrame here'        
        is_18plus = ~Day.Person.data[CHILD_AGE_COL].isin(CHILD_AGE_CODES)
        
        if altday_trips.any() and is_18plus.all():            
            self.new_trips.append(
                Trip.impute_from_altday(person_trips[altday_trips].iloc[0])
            )
            
            return
        
        # 3) Otherwise, Create a completely new school trip for that person
        self.new_trips.append(
            Trip.impute_new_school_trip()
        )
      
        return
               
    def is_missing_school_trip(self, Day: DayManagerClass, person: pd.DataFrame) -> bool:
        
        """
        Checks if the trips for the person on that day are missing any school trips.
        
        Args:
            Day (DayManagerClass): the initialized Day class
            person_day_trips: DataFrame trips table.

        Returns: boolean True/False
        """
        
        # Skip if person not proxy (is adult)        
        if not person[CHILD_AGE_COL].isin(CHILD_AGE_CODES).iloc[0]:
            return False
                
        # Skip if person is pre-school age and school_type is not preschool (does not attend preschool)
        is_preschool_age = person[PRESCHOOL_AGE_COL].isin(PRESCHOOL_AGE_CODES).iloc[0]
        is_in_preschool = person[PRESCHOOL_TYPE_COL].isin(PRESCHOOL_TYPE_CODES).iloc[0]
        if is_preschool_age and not is_in_preschool:
            return False
        
        # Skip if person (child) already has school destination
        person_day_trips = Day.get_related('trip', on=['hh_id', 'day_num'])
        if person_day_trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES).any():
            return False
        
        return True
    
    