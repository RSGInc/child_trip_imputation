"""
CLASS MANAGERS TO BE PLACED IN THEIR OWN FILES AS THEY GROW TOO LARGE
"""
import pandas as pd
import utils.io
from utils.io import DBIO

import settings

"""
HTS Class managers inheritance structure:

HouseholdManagerClass: Contains dict for all persons in the household
├─── {person_id: PersonManagerClass} - Inherits household, contains dict for all days for a person
|               ├── {day_id: DayManagerClass} - Inherits person, contains dict for all tours for a day
|               |           ├── {tour_id: TourManagerClass} - Iherits day, contains dict for all trips for a tour
|               |           |              ├── {trip_id: TripManagerClass} - Inherits tour, contains trip data
...

This enables tree-like traversal of the data so only the relevant data is available at each iteration.
E.g., from a trip, person data can be accessed via TripMgr.TourMgr.DayMgr.PersonMgr.data
which will return the person data for the person who made the trip.

We're going to skip Tour manager for now until it can be fully implemented.

"""


# CONSTANTS
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES
HHMEMBER_PREFIX = COLNAMES['HHMEMBER']
SCHOOL_MODE = COLNAMES['SCHOOL_MODE']


class ManagerClass:
    """
    Manager base class to avoid repeating myself    
    """
    
    # The Database IO object is part of the class
    DBIO: utils.io.IO = DBIO
    
    def __init__(self, data: pd.DataFrame|None) -> None:
        
        assert isinstance(data, pd.DataFrame|None), 'Class manager data must be a pandas dataframe'
        if data is not None:
            assert data.shape[0] == 1, 'Class manager data must be a single row pandas dataframe'        
    
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
        assert isinstance(self.data, pd.DataFrame), 'Class manager data must be a pandas dataframe when fetching related data'
        assert isinstance(on, str|list) or on is None, 'Class manager related data on must be a string or None'
        assert self.data.index.name in df.columns, 'Class manager related data must have a common index with data'            

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
    def __init__(self, trip: pd.DataFrame|None, Day: DayManagerClass, Tour: TourManagerClass|None) -> None:        
        super().__init__(trip)
        self.Tour = Tour
        self.Day = Day
    
    def impute_from_escort(self, escort_trip: pd.DataFrame):
        # TODO: Implement
        pass
    
    def impute_from_altday(self, altday_trip: pd.DataFrame):
        # TODO: Implement
        pass
    
    def impute_new_school_trip(self):
        # TODO: Implement
        assert isinstance(self.Day.Person.data, pd.DataFrame), 'Day.Person.data must be a DataFrame'
        
        list(self.Day.Person.data.columns)
        self.Day.Person.data[SCHOOL_MODE]
        pass
    
    def populate(self, trip_trips):
        pass
    
