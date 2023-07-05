import pandas as pd
from school_trips.base_manager import ManagerClass

class HouseholdManagerClass(ManagerClass):
    def __init__(self, household: pd.DataFrame) -> None:
        super().__init__(household)
    