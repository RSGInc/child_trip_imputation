import os
import pandas as pd
import pandera as pa
import psycopg2 as pg
from psycopg2.extensions import connection
import settings


class IO:
    """
    This class is intended to hold data input/output methods. 
    It should be inherited by higher order classes to make the functions available.
    """
    
    def __init__(self) -> None:
                    
        # Create receiving folders if not existing        
        if settings.CACHE_DIR and not os.path.isdir(settings.CACHE_DIR):
            os.makedirs(settings.CACHE_DIR)
            
        if settings.OUTPUT_DIR and not os.path.isdir(settings.OUTPUT_DIR):
            os.makedirs(settings.OUTPUT_DIR)
            
    def list_tables(self):
        assert isinstance(settings.TABLES, dict)        
        return list(settings.TABLES.keys())
    
    def list_indices(self):
        assert isinstance(settings.TABLES, dict)
        tables = settings.TABLES
        return {k: v.get('index') for k, v in tables.items()}
    
    def index_frame(self):
        households_df = self.get_table('household')
        persons_df = self.get_table('person')
        days_df = self.get_table('day')
        trips_df = self.get_table('trip')
        
        person_index = persons_df[[households_df.index.name]].reset_index().astype(str)
        day_index = days_df[[persons_df.index.name, 'day_num']].reset_index().astype(str)
        trip_index = trips_df[[days_df.index.name]].reset_index().astype(str)
        
        indices_df = pd.merge(person_index, day_index, how='outer').merge(trip_index, how='outer')
        
        return indices_df

    
    def get_table(self, table):
        """
        Returns pandas DataFrame table for the requested table. 
        Checks first if already loaded, then checks cached data, then fetches from POPS.

        Args:
            table (str): Canonical table name (e.g., households) 
            mapped to POPS table name (e.g., w_rm_hh) in settings.yaml
        """
        
        if not hasattr(self, table):       
            
            # Validate parameters            
            assert isinstance(settings.CACHE_DIR, str), 'CACHE_DIR must be a string path'
            assert isinstance(settings.TABLES, dict), 'TABLES must be a dictionary of canonical table names and POPS name'
            assert table in settings.TABLES.keys(), f"{table} table is required in setting.yaml under TABLES"            
            assert isinstance(settings.TABLES.get(table), dict), 'TABLES item must be dict with name and index'
            
            # Extract tables settings item
            table_item = settings.TABLES.get(table)
            
            # Assert it is not empty
            assert table_item is not None, f'TABLE {table} must not be empty!'
            assert table_item.get('name'), 'TABLE dictionary must have POPS "name"'            
                           
            table_name = table_item.get('name')
            table_index = table_item.get('index')          
            
            # Where to store cached data as parquet
            cache_path = os.path.join(settings.CACHE_DIR, f'{table_name}.parquet')
            
            # Read cached file if available
            if os.path.isfile(cache_path):
                print(f'Load {table_name} from cache')
                df = pd.read_parquet(cache_path)

            # Else, fetch from POPS
            else:
                print(f'Fetch {table_name} from POPS')
                conn = pg.connect(
                    database=settings.PG_DB,
                    user=settings.PG_USER,
                    password=settings.PG_PWD,
                    host=settings.PG_HOST,
                    port=settings.PG_PORT,
                    keepalives_idle=600
                )
  
                df = pd.read_sql(f"select * from {settings.STUDY_SCHEMA}.{table_name}", con=conn)
                conn.close()
                
                if table_index:
                    df.set_index(table_index, inplace=True)
                    
                df.to_parquet(cache_path)                          
            
            setattr(self, table, df)                        
            
        else:
            df = getattr(self, table)
        
        # Extract pandera schema
        if hasattr(self, f'schema_{table}'):
            schema = getattr(self, f'schema_{table}')
        else:
            schema = pa.infer_schema(df)
            setattr(self, f'schema_{table}', schema)  
               
        assert isinstance(schema, pa.DataFrameSchema)
            
        return df
    
    def to_csv(self):
        pass
    
# Debugging
if __name__ == "__main__":
    IO().get_table('households')
    