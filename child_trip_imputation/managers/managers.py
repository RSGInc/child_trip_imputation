"""
THIS IS A TEMPORARY DEV FILE
CLASS MANAGERS WILL BE PLACED IN THEIR OWN FILES

"""
import pandas as pd

class HouseholdManagerClass:
    def __init__(self, household: pd.DataFrame) -> None:
        self.household = household        

class PersonManagerClass:
    def __init__(self, person: pd.DataFrame, Household: HouseholdManagerClass) -> None:
        self.person = person
        self.Household = Household

class DayManagerClass:
    def __init__(self, day: pd.DataFrame, Person: PersonManagerClass) -> None:
        self.day = day
        self.Person = Person

class TripManagerClass:
    def __init__(self, trip: pd.DataFrame, Day: DayManagerClass) -> None:
        self.trip = trip
        self.Day = Day
        
    def find_imputations(self):
        pass
    
    def impute_nonproxy(self):
        pass
    
    def impute_attendance(self):
        pass
    
    def impute_stayed_home(self):
        pass
    
    def report_bad_impute(self):
        pass
    

class TourManagerClass:
    def __init__(self, tour: pd.DataFrame, Trip: TripManagerClass) -> None:
        self.tour = tour
        self.Trip = Trip