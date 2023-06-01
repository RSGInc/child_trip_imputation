import os
import pandas as pd
import pandera as pa
import psycopg2 as pg
from psycopg2.extensions import connection
import settings


class IO:
    def setup_local_dirs(self):
        if settings.CACHE_DIR:
            os.makedirs(settings.CACHE_DIR)
        if settings.OUTPUT_DIR:
            os.makedirs(settings.OUTPUT_DIR)

    
    def get_table(self, table):
        """
        Returns pandas DataFrame table for the requested table. Checks first if already loaded, then checks cached data, then fetches from POPS.

        Args:
            table (str): Canonical table name (e.g., households) mapped to POPS table name (e.g., w_rm_hh) in settings.yaml
        """
        
        if not hasattr(self, table):       
                        
            assert isinstance(settings.TABLES, dict)
            assert isinstance(settings.CACHE_DIR, str)        
            assert table in settings.TABLES.keys(), f"{table} table is required in setting.yaml under TABLES"

            table_name = settings.TABLES.get(table)            
            cache_path = os.path.join(settings.CACHE_DIR, f'{table_name}.parquet')
            
            if os.path.isfile(cache_path):
                df = pd.read_parquet(cache_path)

            else:
                conn = pg.connect(
                    database=settings.PG_DB,
                    user=settings.PG_USER,
                    password=settings.PG_PWD,
                    host=settings.PG_HOST,
                    port=settings.PG_PORT,
                    keepalives_idle=600
                )        
  
                df = pd.read_sql(f"select * from {settings.STUDY_SCHEMA}.{table_name}", conn)
                
                df.to_parquet(cache_path)
                conn.close()                       
            
            
            setattr(self, table, df)                        
            
        else:
            df = getattr(self, table)
            
        if hasattr(self, f'schema_{table}'):
            schema = getattr(self, f'schema_{table}')
        else:
            schema = pa.infer_schema(df)
            setattr(self, f'schema_{table}', schema)  
               
        assert isinstance(schema, pa.DataFrameSchema)
            
        return df
    
    
# Debugging
if __name__ == "__main__":
    IO().get_table('households')
    