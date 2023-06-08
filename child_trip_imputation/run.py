# External packages
import logging
import pandas as pd
from tqdm import tqdm # There is a parallel version of tqdm called pqdm

# Internal imports
from utils.io import IO
import settings
from utils.misc import is_missing_school_trip, get_index_name
from methods.trips_to_tours import bulk_trip_to_tours
from methods.impute_nonproxy import impute_nonproxy
from methods.impute_school_trips import impute_school_trips
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
       
    def run_imputation(self) -> None:

        # Tables and index
        households_df, hh_id_col = self.get_table('household'), get_index_name('household')
        persons_df, per_id_col = self.get_table('person'), get_index_name('person')
        days_df, day_id_col = self.get_table('day'), get_index_name('day')
        trips_df, trip_id_col = self.get_table('trip'), get_index_name('trip')
        tour_id_col = 'tour_id'
        
        codebook = self.get_table('codebook')        
        # One off fix, drop duplicated codebook values -- should be fixed in postgres    
        codebook = codebook[~codebook.reset_index()[['name', 'value']].duplicated().to_numpy()]
        
        # # Create vectorized nested loop index
        # index_df = self.index_frame()
        
        # Bulk create tour IDs per person so they can be determined as joint tours later
        trips_df = bulk_trip_to_tours(trips_df)
        
        # Begin outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(households_df.groupby(hh_id_col)):
            Household = HouseholdManagerClass(hh)
            hh_persons = persons_df[persons_df[hh_id_col] == hh_id]
            
            # level 1 loop over persons in household, adding Household to Person manager 
            for person_id, person in hh_persons.groupby(per_id_col):                                
                Person = PersonManagerClass(person, Household)
                person_days = days_df[days_df[per_id_col] == person_id]
                
                # level 2 loop over days for persons, adding Person to Day manager
                for (day_id, day_num), day in person_days.groupby([day_id_col,'day_num']):
                    Day = DayManagerClass(day, Person)
                    
                    person_day_trips = trips_df[trips_df[day_id_col] == day_id]
                    household_day_trips = trips_df[(trips_df.day_num == day_num) & (trips_df[hh_id_col] == hh_id)]
            
                    # Level 3 loop over tours
                    for tour_id, tour_trips in person_day_trips.groupby(tour_id_col):
                        # There is no tour table yet, create an empty dummy to be populated
                        tour = pd.DataFrame(
                            data={day_id_col: day_id, hh_id_col: hh_id, per_id_col: person_id}, 
                            index=pd.Index([tour_id], name=tour_id_col)
                            )
                        
                        Tour = TourManagerClass(tour, Day)
                        Tour.populate_tour(tour_trips)
            
                        # Skip if imputation is not required
                        if is_missing_school_trip(Day, person_day_trips):
                            
                            # 1) Impute from non-proxy
                            impute_nonproxy(tour_trips, household_day_trips)
                                                        
                            # 2) Impute missing school trips
                            impute_school_trips()

    
    def report_bad_impute(self):
        pass
        

def impute_stayed_home(self):
    pass
    

            


if __name__ == "__main__":
    CTI = Imputation()
    