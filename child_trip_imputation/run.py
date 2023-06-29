# External packages
import logging
import pandas as pd

# Internal imports
from utils.io import DBIO
import settings
from utils.trips_to_tours import bulk_trip_to_tours
from nonproxy.impute import ImputeNonProxyTrips
from school_trips.impute import ImputeSchoolTrips


"""
1. Find persons that need imputation
2. Imputation cases:
    •	[impute_nonproxy] Non-proxy household members said were accompanied by them
    •	[impute_attendance] No trip to school, but student is reported to have attended or attendance not reported
    •	[report_bad_impute] Unable to import, report warning/summary
"""

# CONSTANTS
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES
JOINT_TRIP_ID_NAME = COLNAMES['JOINT_TRIP_ID_NAME']


class Imputation(ImputeNonProxyTrips, ImputeSchoolTrips):
        
    def __init__(self) -> None:
        """
        This class inherits methods from ImputeNonProxyTrips and ImputeSchoolTrips.
        The methods are run in the order specified in settings.STEPS using the redefined wrappers below 
        that interact with the DBIO object to enable caching and to allow the other functions to run standalone
        """
        assert isinstance(settings.STEPS, list)
        
        for step in settings.STEPS:
            # If step already run, load from cache
            if settings.RESUME_AFTER and step in DBIO.cache_log.index:
                table = DBIO.cache_log.loc[step, 'table']
                cache_path = DBIO.cache_log.loc[step, 'cached_table']
                assert isinstance(cache_path, str), f'Cached table path for step {step} is not a string'                
                df = DBIO.get_table(table, step)
                if df is None:
                    # If the table is not in the DBIO object, re-run the step
                    getattr(self, step)()
            else:            
                getattr(self, step)()
    
    # Local function
    def report_bad_impute(self) -> None:
        pass
            
    def validate_table_relations(self) -> None:
        pass
    
    # These inherited functions are wrappers to interact with the DBIO object
    def create_tours(self) -> None:
        """
        Append tours id to trips, creates tours table from trips table and updates DB object.
        """
        trips_df = DBIO.get_table('trip')
        
        # Bulk create tour IDs per person so they can be determined as joint tours later
        trips_df = bulk_trip_to_tours(trips_df)
        
        # Update the trip table in DB to include tour IDs
        DBIO.update_table('trip', trips_df)
                   
        assert isinstance(trips_df, pd.DataFrame)
        
        # Get the first trip for each tour to create a tours table
        tours_df = trips_df.groupby('tour_id').first()
        
        # Manually add day_num and tour_num to tours_df result            
        cols = [str(settings.get_index_name(x)) for x in ['day', 'household', 'person']]
        cols += ['day_num', 'tour_num']
         
        assert set(tours_df.columns).intersection(set(cols)), f'Columns {cols} not in tours_df'

        # Update the tours table in DB
        DBIO.update_table('tour', tours_df[cols], step_name = 'create_tours')

    def flag_unreported_joint_trips(self) -> None:
        """
        Flag all unreported joint trips and update DB object. 
        """
        assert isinstance(settings.JOINT_TRIP_BUFFER, dict)        
        kwargs = { 'trips_df': DBIO.get_table('trip'), **settings.JOINT_TRIP_BUFFER}        
        flagged_trips_df = super(Imputation, self).flag_unreported_joint_trips(**kwargs)
        
        # Update the class DBIO object data.
        DBIO.update_table('trip', flagged_trips_df, step_name = 'flag_unreported_joint_trips')
        
        return

    def impute_reported_joint_trips(self) -> None:
        """
        Impute all missing reported joint trips and update DB object.
        """        
        kwargs = {'persons_df': DBIO.get_table('person'), 'trips_df': DBIO.get_table('trip')}        
        updated_trips_df = super(Imputation, self).impute_reported_joint_trips(**kwargs)
        
        # Update the class DBIO object data.
        DBIO.update_table('trip', updated_trips_df, step_name = 'impute_reported_joint_trips')
        
        return

    def impute_school_trips(self) -> None:
        """
        Impute all missing school trips and update DB object.
        """        
        
        assert isinstance(settings.JOINT_TRIP_BUFFER, dict)
        
        kwargs = {
            'households_df': DBIO.get_table('household'),
            # 'persons_df': DBIO.get_table('person'),
            # 'trips_df': DBIO.get_table('trip'),
            # **settings.JOINT_TRIP_BUFFER
            }
        
        imputed_school_trips_df = super(Imputation, self).impute_school_trips(**kwargs)
        
        # Update the class DBIO object data.
        assert isinstance(imputed_school_trips_df, pd.DataFrame), f'Imputed school trips is not a DataFrame'
        DBIO.update_table('trip', imputed_school_trips_df, step_name = 'impute_school_trips')

    def reconcile_id_sets(self) -> None:
        # some of the ID sequences are inconsistent, so we need to reconcile them
        # E.g., trip_id starts at ...003 but trip_num starts at 1
        pass
    
    def summaries(self) -> None:
        """
        Create summary tables and update DB object.
        """
        
        # Summarize the number of flagged joint trips and corrected hh members on those joint trips
        joint_trips_df = DBIO.get_table('trip', step='flag_unreported_joint_trips')
        assert joint_trips_df is not None, f'Joint trips table is missing'
        
        DBIO.summaries['total_joint_trips'] = joint_trips_df[joint_trips_df[JOINT_TRIP_ID_NAME] != 995].shape[0]
        DBIO.summaries['joint_trips'] = joint_trips_df.loc[joint_trips_df[JOINT_TRIP_ID_NAME] != 995, JOINT_TRIP_ID_NAME].nunique()
        DBIO.summaries['unreported_joint_trips'] = joint_trips_df['corrected_hh_members'].sum()
        
        # Summarize the number of imputed joint trips
        imputed_joint_trips_df = DBIO.get_table('trip', step='impute_reported_joint_trips')
        assert imputed_joint_trips_df is not None, f'Imputed joint trips table is missing'
        
        DBIO.summaries['imputed_joint_trips'] = imputed_joint_trips_df['imputed_joint_trip'].sum()
        
        

if __name__ == "__main__":
    IMP = Imputation()
    