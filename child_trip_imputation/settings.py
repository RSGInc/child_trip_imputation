"""
This module creates a global importable settings Namespace.
Other modules can easily inherit global settings by simply importing this module.
"""

import yaml
from dotenv import dotenv_values

# Load .env variables into proto namespace dictionary along with the settings.yaml
with open('settings.yaml', 'r') as file:
    SETTINGS = {**yaml.safe_load(file), **dotenv_values(".env")}

# Assign any variables in the settings to the environment namespace
for k, v in SETTINGS.items():
    globals()[k] = v

"""
Passing settings into global namespace, you can set defaults here.
I am declaring the variables explicitly here for type checking despite being assigned dynamically above
"""
PG_HOST = SETTINGS.get('PG_HOST', 'pops.rsginc.com') # Set defaults like this
STUDY_SCHEMA = SETTINGS.get('STUDY_SCHEMA')
PG_DB = SETTINGS.get('PG_DB')
PG_PORT = SETTINGS.get('PG_PORT')
PG_USER = SETTINGS.get('PG_USER')
PG_PWD = SETTINGS.get('PG_PWD')
TABLES = SETTINGS.get('TABLES')
CACHE_DIR = SETTINGS.get('CACHE_DIR')
OUTPUT_DIR = SETTINGS.get('OUTPUT_DIR')
CODES = SETTINGS.get('CODES')


# Assertions can come here
assert isinstance(TABLES, dict)
for table in ['household', 'person', 'day', 'codebook']:
    assert table in TABLES.keys(), f'{table} must be defined in settings.yaml!'