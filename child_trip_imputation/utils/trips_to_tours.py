from child_trip_imputation import settings
from utils.misc import get_codes

def bulk_trip_to_tours(trips):
    
    assert isinstance(settings.CODES, dict) 
    HOME_PURPOSES_COL, HOME_PURPOSES_CODES = get_codes('HOME_PURPOSE')

    trips[HOME_PURPOSES_COL].isin(HOME_PURPOSES_CODES)

    return trips
