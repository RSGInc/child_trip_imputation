# External packages
import logging
import pandas as pd
from tqdm import tqdm # There is a parallel version of tqdm called pqdm

# Internal imports
from utils.io import IO
from utils import settings
from utils.misc import get_codes
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
        
        
        # Begin outer loop on households, initialize Household manager
        for hh_id, hh in tqdm(households_df.groupby('hh_id')):
            Household = HouseholdManagerClass(hh)
            hh_persons = persons_df[persons_df.hh_id == hh_id]
            
            # Inner loop over persons in household, adding Household to Person manager 
            for person_id, person in hh_persons.groupby('person_id'):                
                                
                Person = PersonManagerClass(person, Household)
                person_days = days_df[days_df.person_id == person_id]
                
                # Inner-inner loop over days for persons, adding Person to Day manager
                for day_id, day in person_days.groupby('day_id'):
                    Day = DayManagerClass(day, Person)
                    day_trips = trips_df[trips_df.day_id == day_id]                    
                    
                    # Skip if imputation is required
                    if not self.is_impute_needed(Day, day_trips):
                        continue                
                        
            
    def is_impute_needed(self, Day, day_trips):
        
        assert isinstance(settings.CODES, dict) 
        CHILD_AGE_COL, CHILD_AGE_CODES = get_codes('CHILD_AGE')
        PRESCHOOL_AGE_COL, PRESCHOOL_AGE_CODES = get_codes('PRESCHOOL_AGE')
        PRESCHOOL_TYPE_COL, PRESCHOOL_TYPE_CODES = get_codes('PRESCHOOL_TYPES')
        
        # Skip if person not proxy (is adult)        
        if not Day.Person.data[CHILD_AGE_COL].isin(CHILD_AGE_CODES).iloc[0]:
            return False
        
        # Skip if person has outbound school trip already
        day_trips
        
        
        
        
        # Skip if person is pre-school age and school_type is not preschool (does not attend preschool)
        is_preschool_age = Day.Person.data[PRESCHOOL_AGE_COL].isin(PRESCHOOL_AGE_CODES).iloc[0]
        is_in_preschool = Day.Person.data[PRESCHOOL_TYPE_COL].isin(PRESCHOOL_TYPE_CODES).iloc[0]
        if is_preschool_age and not is_in_preschool:
            return False
        
        
        return True 
        
        
        
    
    def impute_nonproxy(self):
        pass
    
    def impute_attendance(self):
        pass
    
    def impute_stayed_home(self):
        pass
    
    def report_bad_impute(self):
        pass
            


if __name__ == "__main__":
    CTI = Imputation()
    