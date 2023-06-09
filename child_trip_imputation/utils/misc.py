import pandas as pd
from managers.managers import DayManagerClass
import settings
from settings import get_codes

def is_missing_school_trip(Day: DayManagerClass, person_day_trips: pd.DataFrame) -> bool:
    
    """
    Checks if the trips for the person on that day are missing any school trips.
    
    Args:
        Day (DayManagerClass): the initialized Day class
        person_day_trips: DataFrame trips table.

    Returns: boolean True/False
    """
        
    assert isinstance(settings.CODES, dict) 
    CHILD_AGE_COL, CHILD_AGE_CODES = get_codes('CHILD_AGE')
    PRESCHOOL_AGE_COL, PRESCHOOL_AGE_CODES = get_codes('PRESCHOOL_AGE')
    PRESCHOOL_TYPE_COL, PRESCHOOL_TYPE_CODES = get_codes('PRESCHOOL_TYPES')
    SCHOOL_PURPOSES_COL, SCHOOL_PURPOSES_CODES = get_codes('SCHOOL_PURPOSES')
    
    # Skip if person not proxy (is adult)        
    if not Day.Person.data[CHILD_AGE_COL].isin(CHILD_AGE_CODES).iloc[0]:
        return False
            
    # Skip if person is pre-school age and school_type is not preschool (does not attend preschool)
    is_preschool_age = Day.Person.data[PRESCHOOL_AGE_COL].isin(PRESCHOOL_AGE_CODES).iloc[0]
    is_in_preschool = Day.Person.data[PRESCHOOL_TYPE_COL].isin(PRESCHOOL_TYPE_CODES).iloc[0]
    if is_preschool_age and not is_in_preschool:
        return False
    
    # Skip if person (child) already has school destination
    if person_day_trips[SCHOOL_PURPOSES_COL].isin(SCHOOL_PURPOSES_CODES).any():
        return False
    
    return True 

def check_spacetime_overlap():
    pass