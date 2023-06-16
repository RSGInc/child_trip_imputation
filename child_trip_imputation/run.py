# External packages
import logging
import pandas as pd
from tqdm import tqdm # There is a parallel version of tqdm called pqdm
from collections.abc import Iterable

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
                table = DBIO.cache_log.loc[step, 'table_name']
                cache_path = DBIO.cache_log.loc[step, 'cached_table']
                assert isinstance(cache_path, str), f'Cached table path for step {step} is not a string'                
                DBIO.get_table(table, step)
            else:            
                getattr(self, step)()
    
    # Local function
    def report_bad_impute(self) -> None:
        pass
            
    def validate_table_relations(self) -> None:
        pass
    
    # These inherited functions are wrappers to interact with the DBIO object
    def create_tours(self) -> None:           
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
        1) Impute all non-proxy joint trips and update DB object.
        This was written as a standalone class so it can be run independently of this imputation class, but able to be iherited easily.
        """        
        
        assert isinstance(settings.JOINT_TRIP_BUFFER, dict)
        
        kwargs = {
            'households_df': DBIO.get_table('household'),
            'persons_df': DBIO.get_table('person'),
            'trips_df': DBIO.get_table('trip')[:100],
            **settings.JOINT_TRIP_BUFFER
            }
        
        imputed_nonproxy_trips_df = super(Imputation, self).impute_school_trips(**kwargs)
        
        # Update the class DBIO object data.
        assert isinstance(imputed_nonproxy_trips_df, pd.DataFrame), f'imputed_nonproxy_trips_df is not a DataFrame'
        DBIO.update_table('trip', imputed_nonproxy_trips_df, step_name = 'impute_nonproxy')

    def reconcile_id_sets(self) -> None:
        # some of the ID sequences are inconsistent, so we need to reconcile them
        # E.g., trip_id starts at ...003 but trip_num starts at 1
        pass

if __name__ == "__main__":
    IMP = Imputation()
    