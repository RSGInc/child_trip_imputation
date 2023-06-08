import pandas as pd
from managers.managers import DayManagerClass
import settings

def get_index_name(table_name: str):
    assert isinstance(settings.TABLES, dict)
    assert table_name in settings.TABLES.keys(), f'{table_name} not in settings.TABLES'
    
    keyvalues = settings.TABLES.get(table_name)

    assert isinstance(keyvalues, dict), f'{table_name} not a dict'            
    assert 'index' in keyvalues.keys()
    
    return keyvalues.get('index')


def get_codes(code_name: str | tuple) -> tuple:
    """
    Asserts and returns the key-values pair for the requested codebook code
    
    Args:
        code_name (str | tuple): name of code pair under CODES in settings.yaml

    Returns:
        tuple: (column name: str, codes: list)
    """
    
    assert isinstance(settings.CODES, dict)
    
    # Unpack tuple if tuple
    if isinstance(code_name, tuple):
        code_name, code_level = code_name
    else:
        code_level = None
        
    assert code_name in settings.CODES.keys(), f'{code_name} not in CODES'
    keyvalues = settings.CODES.get(code_name)
    
    assert isinstance(keyvalues, dict), f'{code_name} not a dict'            
    
    # Unpack second level if tuple
    if code_level:
        assert code_level in keyvalues.keys()
        keyvalues = keyvalues.get(code_level)
    
    assert isinstance(keyvalues, dict), f'{code_name} not a dict'            

    # Unpack
    (col, codes), = keyvalues.items()
    
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
        