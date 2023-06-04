"""
THIS IS A TEMPORARY DEV FILE
CLASS MANAGERS WILL BE PLACED IN THEIR OWN FILES AS THEY GROW TOO LARGE

"""
import pandas as pd

"""
HTS Class managers are nested classes analogous to:

HouseholdManagerClass
└── PersonManagerClass
    └── DayManagerClass
        └── TourManagerClass: We'll use a day as tour for now until we have trip linking in place
            └── TripManagerClass
"""

class HouseholdManagerClass:
    def __init__(self, household: pd.DataFrame) -> None:
        self.data = household        

class PersonManagerClass:
    def __init__(self, person: pd.DataFrame, Household: HouseholdManagerClass) -> None:
        self.data = person
        self.Household = Household

class DayManagerClass:
    def __init__(self, day: pd.DataFrame, Person: PersonManagerClass) -> None:
        self.data = day
        self.Person = Person    

class TourManagerClass:
    def __init__(self, tour: pd.DataFrame, Day: DayManagerClass) -> None:
        self.data = tour
        self.Day = Day
        
class TripManagerClass:
    def __init__(self, trip: pd.DataFrame, Tour: TourManagerClass) -> None:
        self.data = trip
        self.Tour = Tour    

