# External packages
import logging
import pandas as pd
from tqdm import tqdm # There is a parallel version of tqdm called pqdm

# Internal imports
from utils.io import DBIO
import settings
from settings import get_index_name
from utils.misc import is_missing_school_trip
from methods.trips_to_tours import bulk_trip_to_tours
from methods.impute_nonproxy import impute_nonproxy
from methods.impute_school_trips import impute_school_trips
from managers.managers import HouseholdManagerClass, PersonManagerClass, DayManagerClass, TripManagerClass, TourManagerClass


"""
1. Find persons that need imputation
2. Imputation cases:
    •	[impute_nonproxy] Non-proxy household members said were accompanied by them
    •	[impute_attendance] No trip to school, but student is reported to have attended or attendance not reported
    •	[report_bad_impute] Unable to import, report warning/summary
"""

class Imputation:
        
    def __init__(self) -> None:        
        self.run_imputation()                
       
    def run_imputation(self) -> None:

        # # Tables and index
        # households_df = DBIO.get_table('household')
        # persons_df = DBIO.get_table('person')
        # days_df,        day_id_col  = DBIO.get_table('day')
        # trips_df,       trip_id_col = DBIO.get_table('trip'), get_index_name('trip')
        # # tours_df,       tour_id_col = DBIO.get_table('tour'), 'tour_id'
        
        # 1) Impute all non-proxy joint trips
        trips_df = impute_nonproxy(DBIO.get_table('person'), DBIO.get_table('trip'))
        
        # Update the class instance data. Any imputed values must be updated in the class instance
        # Updates in the class managers are local and not reflected in the class instance
        DBIO.update_table('trip', trips_df)
        
        # Begin outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(DBIO.get_table('household').groupby(level=0)):
            Household = HouseholdManagerClass(hh)
            
            # Get persons in household
            household_person = Household.get_related('person')
            assert isinstance(household_person, pd.DataFrame)
            
            # Initialize all person-day-tour-trip managers into a dictionary that can be accessed by person ID
            # Persons = {k: PersonManagerClass(df, Household) for k, df in household_person.groupby(level=0)}
                        
            # level 1 loop over persons in household, adding Household to Person manager
            for person_id, person in household_person.groupby(level=0):                        
                Person = PersonManagerClass(person, Household)
                person_days = Person.get_related('day')
                            
                # level 2 loop over days for persons, adding Person to Day manager
                for day_id, day in person_days.groupby(level=0):
                    Day = DayManagerClass(day, Person)
                    person_day_trips = Day.get_related('trip', on=['hh_id', 'day_num'])
                    person_day_tours = Day.get_related('tour', on=['hh_id', 'day_num'])
                    assert isinstance(person_day_trips, pd.DataFrame)

                    # Skip if imputation is not required
                    if is_missing_school_trip(Day, person_day_trips):
                        # 2) Impute missing school trips
                        impute_school_trips()                    
                
                    # Level 3 loop over tours to populate tours
                    for tour_id, tour in person_day_tours.groupby(level=0):
                        # There is no tour table yet, create an empty dummy to be populated
                        Tour = TourManagerClass(tour, Day)
                        person_day_tour_trips = Tour.get_related('trip', on=['hh_id', 'day_num', 'tour_num'])
                        Tour.populate_tour(person_day_tour_trips)
        


    
    def report_bad_impute(self) -> None:
        pass
            
    def validate_table_relations(self) -> None:
        pass

if __name__ == "__main__":
    IMP = Imputation()
    