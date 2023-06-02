from utils.io import IO
from utils import settings
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
        codebook = codebook[~codebook.reset_index()[['name', 'value']].duplicated().values]
        
        households_df.index.name in persons_df.columns
        
        
        # Begin outer loop on households
        for hh_id, hh in households_df.groupby('hh_id'):
            Household = HouseholdManagerClass(hh)
            hh_persons = persons_df[persons_df.hh_id == hh_id]

            for person_id, person in hh_persons.groupby('person_id'):                            
                Person = PersonManagerClass(person, Household)
                person_days = days_df[days_df.person_id == person_id]
                
                for day_id, day in person_days.groupby('day_id'):
                    Day = DayManagerClass(day, Person)
                    day_trips = trips_df[trips_df.day_id == day_id]
                    
                    for trip_id, trip in day_trips.groupby('trip_id'):
                        Trip = TripManagerClass(trip, Day)
                        
                
            
            
            
        
        
        # Skip 18+
        # person.age
        


if __name__ == "__main__":
    CTI = Imputation()
    