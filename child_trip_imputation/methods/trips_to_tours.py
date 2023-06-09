import pandas as pd
import settings
from settings import get_codes, get_index_name

def bulk_trip_to_tours(trips) -> pd.DataFrame:
    print('Determining tours from trips')
    
    assert isinstance(settings.CODES, dict) 
    HOME_PURPOSES_COL, HOME_PURPOSES_CODES = get_codes(('HOME_PURPOSE', 'ORIGIN'))   
    
    # pre-sort by person id then trip number
    trips = trips.sort_values([get_index_name('person'), 'trip_num'])
    
    # Initialize integer id vector
    trips['tour_num'] = 0
    
    # If it's the first trip for a person, then new tour start
    trips.loc[trips.trip_num==1, 'tour_num'] = 1

    # If the destination purpose is home then it is a new tour
    trips.loc[trips[HOME_PURPOSES_COL].isin(HOME_PURPOSES_CODES), 'tour_num'] = 1

    # Determine tour num as cumulative sum per person, which iterates based on the above conditions    
    idx = get_index_name('person')    
    trips.tour_num = trips.groupby(idx).tour_num.cumsum()
    
    # Generate new tour_id
    trips['tour_id'] = trips.day_id.astype(str) + trips.tour_num.astype(str).str.zfill(2)
    
    return trips

