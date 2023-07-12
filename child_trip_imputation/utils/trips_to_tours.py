import pandas as pd
import settings

assert isinstance(settings.CODES, dict) 
HOME_PURPOSES_COL, HOME_PURPOSES_CODES = settings.get_codes(('HOME_PURPOSE', 'ORIGIN'))
PERSON_ID_NAME = settings.get_index_name('person')

COLNAMES = settings.COLUMN_NAMES
TRIPNUM_COL = COLNAMES['TRIPNUM']

assert isinstance(PERSON_ID_NAME, str), f'person index name not a string'


def bulk_trip_to_tours(trips: pd.DataFrame) -> pd.DataFrame:
    """
    This is a standalone function that takes a trips dataframe and returns a trips dataframe with labeled tours.
    The only dependency is the settings.py file, which is used to determine the index name and the home purposes.

    Args:
        trips (pd.DataFrame): trip dataframe

    Returns:
        pd.DataFrame: trip dataframe with labeled tours
    """
    
    
    print('Determining tours from trips')
    

    # pre-sort by person id then trip number
    trips = trips.sort_values([PERSON_ID_NAME, 'trip_num'])
    
    # Initialize integer id vector
    trips['tour_num'] = 0
    
    # If it's the first trip for a person, then new tour start
    trips.loc[trips.trip_num==1, 'tour_num'] = 1

    # If the destination purpose is home then it is a new tour
    # Do we need to consider subtours here?  If so, what's the criteria? (just atwork?)
    trips.loc[trips[HOME_PURPOSES_COL].isin(HOME_PURPOSES_CODES), 'tour_num'] = 1

    # Determine tour num as cumulative sum per person, which iterates based on the above conditions    
    trips.tour_num = trips.groupby(PERSON_ID_NAME).tour_num.cumsum()
    
    # Generate new tour_id
    trips['tour_id'] = trips.day_id.astype(str) + trips.tour_num.astype(str).str.zfill(2)
    
    return trips

