"""
THIS IS A TEMPORARY DEV FILE
CLASS MANAGERS WILL BE PLACED IN THEIR OWN FILES AS THEY GROW TOO LARGE

"""
import pandas as pd
import utils.io
from utils.io import DBIO

"""
HTS Class managers inheritance structure:

HouseholdManagerClass: Contains dict for all persons in the household
├─── {person_id: PersonManagerClass} - Inherits household, contains dict for all days for a person
|               ├── {day_id: DayManagerClass} - Inherits person, contains dict for all tours for a day
|               |           ├── {tour_id: TourManagerClass} - Iherits day, contains dict for all trips for a tour
|               |           |              ├── {trip_id: TripManagerClass} - Inherits tour, contains trip data
...
"""


class ManagerClass:
    """
    Manager base class to avoid repeating myself    
    """
    
    # The Database IO object is part of the class
    DBIO: utils.io.IO = DBIO
    
    def __init__(self, data: pd.DataFrame) -> None:
        
        assert data.shape[0] == 1, 'Class manager data must be a single row pandas dataframe'
        assert isinstance(data, pd.DataFrame), 'Class manager data must be a pandas dataframe'
    
        self.data = data
        
            
    def get_related(self, related: str, on: str|list|None = None) -> pd.DataFrame:
        """
        Fetches related data from the class manager data

        Args:
            related (str): desired related data table name, must have column with a common index with data
            on (str): column name to join on, default is index name

        Returns:
            pd.DataFrame: related data table
        """
        df = self.DBIO.get_table(related)
        
        assert isinstance(df, pd.DataFrame), f'{related} table is not a DataFrame'
        assert self.data.index.name in df.columns, 'Class manager related data must have a common index with data'            
        assert isinstance(df, pd.DataFrame), 'Class manager related data must be a pandas dataframe'
        assert isinstance(on, str|list) or on is None, 'Class manager related data on must be a string or None'

        if on is None:
            on_cols = self.data.index.name
            on_vals = self.data.index
            related_df = df[df[on_cols].isin(on_vals)]            
            
        else:            
            on = on if isinstance(on, list) else [on]
            assert all(isinstance(s, str) for s in on), 'Class manager related data on must be a list of strings'                
            
            on_vals = self.data[on]
            related_df = pd.merge(df.reset_index(), on_vals, on=on).set_index(df.index.name)            
            
        return related_df 
        

class HouseholdManagerClass(ManagerClass):
    def __init__(self, household: pd.DataFrame) -> None:
        super().__init__(household)
    

class PersonManagerClass(ManagerClass):
    def __init__(self, person: pd.DataFrame, Household: HouseholdManagerClass) -> None:
        super().__init__(person)
        self.Household = Household


class DayManagerClass(ManagerClass):
    def __init__(self, day: pd.DataFrame, Person: PersonManagerClass) -> None:
        super().__init__(day)
        self.Person = Person     

class TourManagerClass(ManagerClass):
    def __init__(self, tour: pd.DataFrame, Day: DayManagerClass) -> None:
        super().__init__(tour)
        self.Day = Day
        
    def populate(self, tour_trips):
        pass
        
class TripManagerClass(ManagerClass):
    # Tour manager is not fully implemented yet, so optional for now
    def __init__(self, trip: pd.DataFrame, Day: DayManagerClass, Tour: TourManagerClass|None) -> None:
        super().__init__(trip)
        self.Tour = Tour
        self.Day = Day

    def populate(self, trip_trips):
        pass
    
