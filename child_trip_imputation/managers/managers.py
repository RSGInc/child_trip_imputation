"""
THIS IS A TEMPORARY DEV FILE
CLASS MANAGERS WILL BE PLACED IN THEIR OWN FILES AS THEY GROW TOO LARGE

"""
import pandas as pd
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
    def __init__(self, data: pd.DataFrame, DBIO=DBIO) -> None:
        
        assert data.shape[0] == 1, 'Class manager data must be a single row pandas dataframe'
        assert isinstance(data, pd.DataFrame), 'Class manager data must be a pandas dataframe'
    
        self.data = data
        self.DBIO = DBIO
        
            
    def get_related(self, related: str, on = None) -> None:
        """
        Fetches related data from the class manager data

        Args:
            related (str): desired related data table name, must have column with a common index with data
            on (str): column name to join on, default is index name

        Returns:
            pd.DataFrame: related data table
        """
        related_df = self.DBIO.get_table(related)
        
        assert self.data.index.name in related_df.columns, 'Class manager related data must have a common index with data'            
        assert isinstance(related_df, pd.DataFrame), 'Class manager related data must be a pandas dataframe'
        assert isinstance(on, str|list) or on is None, 'Class manager related data on must be a string or None'

        if on is None:
            on = self.data.index.name
            on_vals = self.data.index
            related_df = related_df[related_df[on].isin(on_vals)]            
            
        else:
            on_vals = self.data[on]
            related_df = related_df.merge(on_vals, on=on)
                    
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
        
    def populate_tour(self, tour_trips):
        pass
        
class TripManagerClass(ManagerClass):
    def __init__(self, trip: pd.DataFrame, Tour: TourManagerClass) -> None:
        super().__init__(trip)
        self.Tour = Tour    

