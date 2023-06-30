from tqdm import tqdm
import pandas as pd

# Internal imports
import settings
from school_trips.managers import HouseholdManagerClass, PersonManagerClass, DayManagerClass, TripManagerClass, TourManagerClass

# Constants
assert isinstance(settings.CODES, dict) 
CHILD_AGE_COL, CHILD_AGE_CODES = settings.get_codes('CHILD_AGE')
PRESCHOOL_AGE_COL, PRESCHOOL_AGE_CODES = settings.get_codes('PRESCHOOL_AGE')
PRESCHOOL_TYPE_COL, PRESCHOOL_TYPE_CODES = settings.get_codes('PRESCHOOL_TYPES')
SCHOOL_PURPOSES_COL, SCHOOL_PURPOSES_CODES = settings.get_codes('SCHOOL_PURPOSES')
ESCORT_PURPOSE_COL, ESCORT_PURPOSE_CODES = settings.get_codes('ESCORT_PURPOSES')

# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES
HHMEMBER_PREFIX = COLNAMES['HHMEMBER']


class ImputeSchoolTrips:
    new_trips = []
    
    def impute_school_trips(self, households_df: pd.DataFrame) -> None:
                
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
                        
    def school_trip_imputation(self, Day: DayManagerClass) -> None:
        # Initialize a new trip manager for the child
        Trip = TripManagerClass(trip=None, Day=Day, Tour=None)
        
        # Includes trips from all other hh members on that day
        hh_day_trips = Day.get_related('trip', on=['hh_id', 'day_num'])
        
        # Does any other household member report escorting trip with student?
        escort_trips = hh_day_trips[ESCORT_PURPOSE_COL].isin(ESCORT_PURPOSE_CODES)
        if escort_trips.any():
            """ 
            - Use characteristics of household member escorting trip: mode, time, number of participants, etc.
            - Use this as the time matching criteria for an allowed time difference to try to match joint tours together.
            - Stop location should be within X miles of school location. (threshold should be a setting)
            """
            # Loop over escort trips, if any, and create a new trip for the child
            # This might have already been taken care of in the "nonproxy" step above                        
            for escort_trip in hh_day_trips[escort_trips].itertuples():                
                self.new_trips.append(
                    Trip.impute_from_escort(escort_trip)
                    )
                
            return     
        
        # Includes trips from all other days for that person
        person_trips = Day.Person.get_related('trip', on = ['hh_id', 'person_num'])
        
        # Else if there a school trip on another day for that child?
        altday_trips = person_trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES)
        if altday_trips.any():
            """
            - Use characteristics of other school trip. If multiple, just take first instance
            - Only applies to children age 18+ that are using rMove.
            """
            self.new_trips.append(
                Trip.impute_from_altday(hh_day_trips[escort_trips].first())
            )
            
            return
        
        # Otherwise, Create a completely new school trip for that person
        
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
    
    