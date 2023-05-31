from argparse import Namespace
import os
import yaml
import pandas as pd
import psycopg2 as pg
from dotenv import dotenv_values


# Load .env variables into namespace
with open('settings.yaml', 'r') as file:
    SETTINGS = Namespace(
        **yaml.safe_load(file),
        **dotenv_values(".env")
        )


class IO:
    def setup_local_dirs(self):
        if 'CACHE_DIR' in SETTINGS:
            os.makedirs(SETTINGS.CACHE_DIR)
        if 'OUTPUT_DIR' in SETTINGS:
            os.makedirs(SETTINGS.OUTPUT_DIR)

    def get_pop_tables(self):
        conn = pg.connect(
            database=SETTINGS.PG_DB,
            user=SETTINGS.PG_USER,
            password=SETTINGS.PG_PWD,
            host=SETTINGS.PG_HOST,
            port=SETTINGS.PG_PORT,
            keepalives_idle=600
        )

        for k in ['households', 'persons', 'trips', 'day']:
            err = f"{k} table is required in setting.yaml under TABLES"
            assert k in SETTINGS.TABLES.keys(), err

            table_name = SETTINGS.TABLES.get(k)
            table = pd.read_sql(f"select * from {SETTINGS.STUDY_SCHEMA}.{table_name}", conn)

            setattr(self, k, table)

        conn.close()

        return

if __name__ == "__main__":
    pass
    