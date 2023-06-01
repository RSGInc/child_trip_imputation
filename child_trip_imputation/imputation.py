from utils.io import IO
from utils import settings


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
        
        hh = self.get_table('households')
        
    
    def find_missing_trips(self):
        pass
    
    
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
    