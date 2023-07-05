import pandas as pd
from school_trips.base_manager import ManagerClass
from school_trips.day import DayManagerClass

class TourManagerClass(ManagerClass):
    def __init__(self, tour: pd.DataFrame, Day: DayManagerClass) -> None:
        super().__init__(tour)
        self.Day = Day
        
    def populate(self, tour_trips):
        pass