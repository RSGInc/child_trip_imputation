
"""
This module creates a global importable settings Namespace.

Other modules can easily inherit global settings by simply importing this module.

"""

import yaml
from dotenv import dotenv_values

# Load .env variables into proto namespace dictionary along with the settings.yaml
with open('settings.yaml', 'r') as file:
    SETTINGS = {**yaml.safe_load(file), **dotenv_values(".env")}

# Passing settings into global namespace, you can set defaults here
STUDY_SCHEMA = SETTINGS.get('STUDY_SCHEMA')
PG_DB = SETTINGS.get('PG_DB')
PG_HOST = SETTINGS.get('PG_HOST')
PG_PORT = SETTINGS.get('PG_PORT')
PG_USER = SETTINGS.get('PG_USER')
PG_PWD = SETTINGS.get('PG_PWD')
TABLES = SETTINGS.get('TABLES')
CACHE_DIR = SETTINGS.get('CACHE_DIR')
OUTPUT_DIR = SETTINGS.get('OUTPUT_DIR')
    