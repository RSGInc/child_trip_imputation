import pandas as pd
from school_trips.base_manager import ManagerClass
from school_trips.household import HouseholdManagerClass

class PersonManagerClass(ManagerClass):
    def __init__(self, person: pd.DataFrame, Household: HouseholdManagerClass) -> None:
        super().__init__(person)
        self.Household = Household
