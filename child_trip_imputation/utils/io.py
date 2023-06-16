import os
import pandas as pd
import pandera as pa
import sqlalchemy
from datetime import datetime

from utils.trips_to_tours import bulk_trip_to_tours
import settings

class IO:
    """
    This class handles all input/output operations for the model and serves as a global "database" object.
    By being a global object, it can be passed to all classes and methods that need to read or write data
    where the updates are immediately available to all other classes and methods without saving to disk.
    """
    
    def __init__(self) -> None:
                    
        # Create receiving folders if not existing        
        if settings.CACHE_DIR and not os.path.isdir(settings.CACHE_DIR):
            os.makedirs(settings.CACHE_DIR)
            
        if settings.OUTPUT_DIR and not os.path.isdir(settings.OUTPUT_DIR):
            os.makedirs(settings.OUTPUT_DIR)
        
        # Initialize log either way. If cache dir is not set, log will not be saved.
        self.log = pd.DataFrame(columns=['step', 'step_name', 'table', 'cached_table', 'timestamp'])
            
        if settings.CACHE_DIR and settings.RESUME_AFTER:
            log_path = os.path.join(settings.CACHE_DIR, 'log.csv')
            
            if os.path.isfile(log_path):            
                self.log = pd.read_csv(log_path)
            else:
                self.log.to_csv(log_path)
            
            
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

    def update_table(self, table, df, step_name=None):
        setattr(self, table, df)
        
        # Save state
        if settings.CACHE_DIR and step_name:            
            # Update log
            self.log[step_name]
            
            # Save log and cache
            cache_path = os.path.join(settings.CACHE_DIR, f'{step_name}_{table}.parquet')
            log_path = os.path.join(settings.CACHE_DIR, 'log.csv')

            self.log.loc[len(self.log)] = [step_name, table, cache_path, datetime.now()]
            
            
            self.log.to_csv(log_path, index=False)
            df.to_parquet(cache_path)
        
        return    
    
    
    def get_table(self, table):
        """
        Returns pandas DataFrame table for the requested table. 
        Checks first if already loaded, then checks cached data, then fetches from POPS.

        Args:
            table (str): Canonical table name (e.g., households) 
            mapped to POPS table name (e.g., w_rm_hh) in settings.yaml
        """
        assert isinstance(settings.TABLES, dict), 'TABLES must be a dictionary of canonical table names and POPS name'
        assert table in settings.TABLES.keys() or hasattr(self, table), f'{table} not in settings.TABLES or loaded in IO object'
                
        
        if not hasattr(self, table):       
            
            table_name, table_index, cache_path = self.validate_table_request(table)          
            
            # Read cached file if available
            if os.path.isfile(cache_path):
                print(f'Load {table_name} from cache')
                df = pd.read_parquet(cache_path)

            # Else, fetch from POPS
            else:
                print(f'Fetch {table_name} from POPS')                
                dbsys = settings.DB_SYS
                database=settings.PG_DB
                username=settings.PG_USER
                password=settings.PG_PWD
                hostname=settings.PG_HOST
                port=settings.PG_PORT
                
                conn_string = f"{dbsys}://{username}:{password}@{hostname}:{port}/{database}"                
                engine = sqlalchemy.create_engine(conn_string)
                df = pd.read_sql(f"select * from {settings.STUDY_SCHEMA}.{table_name}", con=engine)
                
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
    
    def validate_table_request(self, table):
        
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
        
        return table_name, table_index, cache_path
    
    def to_csv(self):
        pass
    
    
DBIO = IO()