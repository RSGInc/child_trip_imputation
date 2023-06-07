import pandas as pd
from child_trip_imputation.run import DayManagerClass
from child_trip_imputation import settings

def get_codes(code_name: str) -> tuple:
    """
    Asserts and returns the key-values pair for the requested codebook code
    
    Args:
        code_name (str): name of code pair under CODES in settings.yaml

    Returns:
        tuple: (column name: str, codes: list)
    """
    assert isinstance(settings.CODES, dict)
    assert code_name in settings.CODES.keys()
    code = settings.CODES.get(code_name)
    
    assert isinstance(code, dict), f'{code_name} not in CODES'
    
    # Unpack
    (col, codes), = code.items()
    
    # Enforce list type
    codes = codes if isinstance(codes, list) else [codes]
    
    return (col, codes)


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
        