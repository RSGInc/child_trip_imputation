import os
import pandas as pd
import pandera as pa
import numpy as np
import sqlalchemy
from datetime import datetime

import settings

class IO:
    """
    This class handles all input/output operations for the model and serves as a global "database" object.
    By being a global object, it can be passed to all classes and methods that need to read or write data
    where the updates are immediately available to all other classes and methods without saving to disk.
    """
    
    summaries = {}
    current_step = None
    table_list = []
    
    def __init__(self) -> None:
                    
        # Create receiving folders if not existing        
        if settings.CACHE_DIR and not os.path.isdir(settings.CACHE_DIR):
            os.makedirs(settings.CACHE_DIR)
            
        if settings.OUTPUT_DIR and not os.path.isdir(settings.OUTPUT_DIR):
            os.makedirs(settings.OUTPUT_DIR)
        
        # Initialize log either way. If cache dir is not set, log will not be saved.        
        dtypes = np.dtype([('index', str), ("table", str), ('timestamp', datetime), ("cached_table", str)])
        index = pd.Index([], name='step_name', dtype=str)
        self.cache_log = pd.DataFrame(np.empty(0, dtype=dtypes), index=index)
        
        # TODO setup a caching system that can be used to resume from a previous step
        if settings.CACHE_DIR and settings.RESUME_AFTER:
            log_path = os.path.join(settings.CACHE_DIR, 'log.csv')
            
            if os.path.isfile(log_path):            
                self.cache_log = pd.read_csv(log_path, parse_dates=['timestamp']).set_index('step_name')
            else:
                self.cache_log.to_csv(log_path)
            
            
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
        
        assert isinstance(households_df, pd.DataFrame)
        assert isinstance(persons_df, pd.DataFrame)
        assert isinstance(days_df, pd.DataFrame)
        assert isinstance(trips_df, pd.DataFrame)
        
        person_index = persons_df[[households_df.index.name]].reset_index().astype(str)
        day_index = days_df[[persons_df.index.name, 'day_num']].reset_index().astype(str)
        trip_index = trips_df[[days_df.index.name]].reset_index().astype(str)
        
        indices_df = pd.merge(person_index, day_index, how='outer').merge(trip_index, how='outer')
        
        return indices_df

    def update_table(self, table: str, df: pd.DataFrame, step_name: str|None = None) -> None:
        """
        This method updates the table in the IO object and saves it to cache if cache dir is set.

        Args:
            table (str): The canonical table name (e.g., households) mapped to POPS table name.
            df (pd.DataFrame): The pandas DataFrame to be saved.
            step_name (str|None, optional): The name of the step that generated the table. Defaults to None.
        """
        setattr(self, table, df)
        
        # Save current state to cache if cache dir is set
        if settings.CACHE_DIR and step_name:            
            # Save log and cache
            cache_path = os.path.join(settings.CACHE_DIR, f'{table}_({step_name}).parquet')
            log_path = os.path.join(settings.CACHE_DIR, 'log.csv')
            
            # Store the new table in cache and update log
            df.to_parquet(cache_path)
            self.cache_log.loc[step_name] = [df.index.name, table, datetime.now(), cache_path]            
            self.cache_log.to_csv(log_path, index=True)            
        
        return    
        
    def get_table(self, table, step: str|None = None):
        """
        Returns pandas DataFrame table for the requested table. 
        Checks first if already loaded, then checks cached data, then fetches from POPS.

        Args:
            table (str): Canonical table name (e.g., households) 
            mapped to POPS table name (e.g., w_rm_hh) in settings.yaml
        """
        assert isinstance(settings.TABLES, dict), 'TABLES must be a dictionary of canonical table names and POPS name'
        
        # Update the cache log if cache dir is set
        assert isinstance(settings.CACHE_DIR, str), f'{settings.CACHE_DIR} must be a string'
        for entry in self.cache_log.itertuples():
            if not os.path.isfile(entry.cached_table):
                self.cache_log.drop(entry.Index, inplace=True)        
                self.cache_log.to_csv(os.path.join(settings.CACHE_DIR, 'log.csv'), index=True)

        # Check if table exists in settings, IO object, or cache log otherwise it's not a real table.
        table_exists = table in settings.TABLES.keys() or hasattr(self, table) or table in self.cache_log.table.to_list()
        
        assert table_exists, f'{table} not in settings.TABLES, loaded in IO object, or in cache log.'        
        
        # If has object and step is complete, otherwise attempt load from cache or POPS
        # or step in self.cache_log.index.to_list()
        if hasattr(self, table) and (step is None or step == self.current_step):
            df = getattr(self, table)
            
        else:            
            # If cache path is specified, use that
            if step:
                cached = self.cache_log.loc[step]                
                table_index, table_name, cache_path = cached[['index', 'table', 'cached_table']].to_list()
                
                # If the cached file was removed, delete the log entry and try again
                if not os.path.isfile(cache_path):
                    self.cache_log.drop(step, inplace=True)
                    assert isinstance(settings.CACHE_DIR, str), f'{settings.CACHE_DIR} must be a string'
                    self.cache_log.to_csv(os.path.join(settings.CACHE_DIR, 'log.csv'), index=True)
                    Warning(f'Cached file {cache_path} not found. Log has been altered, please re-run the step.')
                    return None
                
            else:
                table_name, table_index, cache_path = self.validate_table_request(table)
            
            # Read cached file if available
            if os.path.isfile(cache_path):
                print(f'Load {table_name}({step}) from cache')
                df = pd.read_parquet(cache_path)
                self.current_step = step

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
                query = f"select * from {settings.STUDY_SCHEMA}.{table_name}"
                df = pd.read_sql(query, con=engine)
                
                if table_index:
                    df.set_index(table_index, inplace=True)
                    
                df.to_parquet(cache_path)                          
            
            setattr(self, table, df)
            self.table_list.append(table)                                 
        
        # Extract pandera schema
        if hasattr(self, f'schema_{table}'):
            schema = getattr(self, f'schema_{table}')
        else:
            schema = pa.infer_schema(df)
            setattr(self, f'schema_{table}', schema)  
               
        assert isinstance(schema, pa.DataFrameSchema), f'schema_{table} must be a pandera DataFrameSchema'
        
        return df
    
    def validate_table_request(self, table: str) -> tuple:
        """
        This method validates the table request and returns the table name, index, and cache path.

        Args:
            table (str): the canonical table name

        Returns:
            tuple(str, str, str): the table name, index, and cache path
        """
        
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
    
    def to_csv(self, df, name):
        assert isinstance(df, pd.DataFrame), 'df must be a pandas DataFrame'
        assert isinstance(name, str), 'name must be a string'
        assert isinstance(settings.OUTPUT_DIR, str), 'settings.OUTPUT_DIR must be a string'
        
        fpath = os.path.join(settings.OUTPUT_DIR, f'{name}.csv')
        df.to_csv(fpath, index=True)
        
        
        
    
    
DBIO = IO()