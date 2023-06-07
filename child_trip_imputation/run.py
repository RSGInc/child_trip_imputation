# External packages
import logging
import pandas as pd
from tqdm import tqdm # There is a parallel version of tqdm called pqdm

# Internal imports
from utils.io import IO
import settings
from utils.misc import get_codes, is_missing_school_trip
from utils.trips_to_tours import bulk_trip_to_tours
from child_trip_imputation.imputation.impute_nonproxy import impute_nonproxy
from managers.managers import HouseholdManagerClass, PersonManagerClass, DayManagerClass, TripManagerClass, TourManagerClass


"""
1. Find persons that need imputation
2. Imputation cases:
    •	[impute_nonproxy] Non-proxy household members said were accompanied by them
    •	[impute_attendance] No trip to school, but student is reported to have attended or attendance not reported
    •	[impute_stayed_home] No school trip, child is reported to have stayed at home
    •	[report_bad_impute] Unable to import, report warning/summary
"""

class Imputation(IO):
    
    def __init__(self) -> None:
        super().__init__()     
        
        self.run_imputation()                
       
    def run_imputation(self):   

        households_df = self.get_table('household')
        persons_df = self.get_table('person')
        days_df = self.get_table('day')
        trips_df = self.get_table('trip')
        codebook = self.get_table('codebook')
        
        # One off fix, drop duplicated codebook values -- should be fixed in postgres    
        codebook = codebook[~codebook.reset_index()[['name', 'value']].duplicated().to_numpy()]
        
        # # Create vectorized nested loop index
        # index_df = self.index_frame()
        
        # TODO: Bulk create tours at person level so they can be determined as joint tours later
        trips_df = bulk_trip_to_tours(trips_df)
        
        # Begin outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(households_df.groupby('hh_id')):
            Household = HouseholdManagerClass(hh)
            hh_persons = persons_df[persons_df.hh_id == hh_id]
            
            # Inner loop over persons in household, adding Household to Person manager 
            for person_id, person in hh_persons.groupby('person_id'):                                
                Person = PersonManagerClass(person, Household)
                person_days = days_df[days_df.person_id == person_id]
                
                # Inner-inner loop over days for persons, adding Person to Day manager
                for (day_id, day_num), day in person_days.groupby(['day_id','day_num']):
                    Day = DayManagerClass(day, Person)
                    person_day_trips = trips_df[trips_df.day_id == day_id]
                    household_day_trips = trips_df[(trips_df.day_num == day_num) & (trips_df.hh_id == hh_id)]
            
                    # Skip if imputation is not required
                    if not is_missing_school_trip(Day, person_day_trips):
                        continue
                    
                    self.impute_child_trips(Day, person_day_trips, household_day_trips)
                    
                    # TODO: Create tours at this point instead of going right to day_trips above
                    # person_day_tours = Day.get_tours(day_trips)
                    # for tour_id, tour in person_day_tours.groupby('tour_id'):
                    #    Tour = TourManagerClass(tour, Day)
                    #    tour_trips = trips_df[trips_df.tour_id == tour_id]                    
                    
    
    def impute_child_trips(self, Day, person_day_trips, household_day_trips):
        # 1) Impute from non-proxy
        impute_nonproxy()
        
        # 2) Impute missing school trips
    
    def report_bad_impute(self):
        pass
        


def impute_attendance(self):
    pass

def impute_stayed_home(self):
    pass
    

            


if __name__ == "__main__":
    CTI = Imputation()
    