from tqdm import tqdm
import pandas as pd

# Internal imports
from managers.managers import HouseholdManagerClass, PersonManagerClass, DayManagerClass, TripManagerClass, TourManagerClass
from utils.misc import is_missing_school_trip
from utils.io import DBIO

# Constants

class ImputeSchoolTrips:
    def impute_school_trips(self, households_df: pd.DataFrame) -> None:
        
        households_df = DBIO.get_table('household')
        
        # Begin outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(households_df.groupby(level=0)):
            Household = HouseholdManagerClass(hh)
            
            # Get persons in household
            household_person = Household.get_related('person')
            
            # level 1 loop over persons in household, adding Household to Person manager
            for person_id, person in household_person.groupby(level=0):
                Person = PersonManagerClass(person, Household)
                person_days = Person.get_related('day')
                            
                # level 2 loop over days for persons, adding Person to Day manager
                for day_id, day in person_days.groupby(level=0):
                    Day = DayManagerClass(day, Person)
                    person_day_trips = Day.get_related('trip', on=['hh_id', 'day_num'])
                    person_day_tours = Day.get_related('tour', on=['hh_id', 'day_num'])

                    # Skip if imputation is not required
                    if is_missing_school_trip(Day, person_day_trips):
                        # 2) Impute missing school trips
                        # Function requires access to trips for all other persons and days in the household                                            
                        
                        # DO THINGS
                        # impute_school_trips(Household, person_day_trips)
                        #
                        
                        pass
                
                    # Level 3 loop over tours to populate tours
                    for tour_id, tour in person_day_tours.groupby(level=0):
                        # There is no tour table yet, create an empty dummy to be populated
                        Tour = TourManagerClass(tour, Day)
                        person_day_tour_trips = Tour.get_related('trip', on=['hh_id', 'day_num', 'tour_num'])
                        Tour.populate_tour(person_day_tour_trips)      