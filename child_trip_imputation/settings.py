"""
This module creates a global importable settings Namespace.

Other modules can easily inherit global settings by simply importing this module.

This module also contains functions for accessing settings.yaml and .env variables
"""

import yaml
from dotenv import dotenv_values

# Load .env variables into proto namespace dictionary along with the settings.yaml
with open('settings.yaml', 'r') as file:    
    SETTINGS = {**yaml.safe_load(file), **dotenv_values(".env")}

# Assign any variables in the settings to the environment namespace
for k, v in SETTINGS.items():
    globals()[k] = v


# Functions for accessing settings.yaml and .env variables
def get_index_name(table_name: str):
    """
    Asserts and returns the index name for the requested table

    Args:
        table_name (str): name of table under TABLES in settings.yaml

    Returns:
        str: name of index column
    """
    
    assert isinstance(TABLES, dict)
    assert table_name in TABLES.keys(), f'{table_name} not in settings.TABLES'
    
    keyvalues = TABLES.get(table_name)

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
    
    assert isinstance(CODES, dict)
    
    # Unpack tuple if tuple
    if isinstance(code_name, tuple):
        code_name, code_level = code_name
    else:
        code_level = None
        
    assert code_name in CODES.keys(), f'{code_name} not in CODES'
    keyvalues = CODES.get(code_name)
    
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


"""
Passing settings into global namespace, you can set defaults here.
I am declaring the variables explicitly here for type checking despite being assigned dynamically above
"""
STEPS = SETTINGS.get('STEPS')
PG_HOST = SETTINGS.get('PG_HOST', 'pops.rsginc.com') # Set defaults like this
DB_SYS = SETTINGS.get('DB_SYS', 'postgresql')
JOINT_TRIP_BUFFER = SETTINGS.get('JOINT_TRIP_BUFFER')
STUDY_SCHEMA = SETTINGS.get('STUDY_SCHEMA')
PG_DB = SETTINGS.get('PG_DB')
PG_PORT = SETTINGS.get('PG_PORT')
PG_USER = SETTINGS.get('PG_USER')
PG_PWD = SETTINGS.get('PG_PWD')
TABLES = SETTINGS.get('TABLES')
CACHE_DIR = SETTINGS.get('CACHE_DIR')
OUTPUT_DIR = SETTINGS.get('OUTPUT_DIR')
CODES = SETTINGS.get('CODES')
RESUME_AFTER = SETTINGS.get('RESUME_AFTER')
IMPUTATION_CONFIGS = SETTINGS.get('IMPUTATION_CONFIGS')
TIME_INCREMENT = SETTINGS.get('TIME_INCREMENT')
LOCAL_TIMEZONE = SETTINGS.get('LOCAL_TIMEZONE')
SCHOOL_TYPE_AGE = SETTINGS.get('SCHOOL_TYPE_AGE')

# Radius of the Earth for Haversine distance calculation
# R = 3963.19 * 5280 #feet
R = 6378100 #meters

 # Column name defaults
COLUMN_NAMES = {
    'PER_ID_NAME': get_index_name('person'),
    'TRIP_ID_NAME': get_index_name('trip'),
    'HH_ID_NAME': get_index_name('household'),
    'DAY_ID_NAME': get_index_name('day'),
    }

COLUMN_NAMES_UPDATE = SETTINGS.get('COLUMN_NAMES', {})
assert isinstance(COLUMN_NAMES_UPDATE, dict)
COLUMN_NAMES.update(**COLUMN_NAMES_UPDATE)


# Assertions can come here
assert isinstance(TABLES, dict)
for table in ['household', 'person', 'day', 'codebook']:
    assert table in TABLES.keys(), f'{table} must be defined in settings.yaml!'


