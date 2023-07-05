import pandas as pd
from school_trips.base_manager import ManagerClass
from school_trips.person import PersonManagerClass

class DayManagerClass(ManagerClass):
    def __init__(self, day: pd.DataFrame, Person: PersonManagerClass) -> None:
        super().__init__(day)
        self.Person = Person     