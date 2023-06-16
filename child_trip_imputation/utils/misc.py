import numpy as np
import pandas as pd
import settings
from managers.managers import DayManagerClass

# Constants
# Extract column names for origin and destination lat/lon
assert isinstance(settings.COLUMN_NAMES, dict), 'COLUMN_NAMES not a dict'
COLNAMES = settings.COLUMN_NAMES

JOINT_TRIPNUM_COL = COLNAMES['JOINT_TRIPNUM_COL']

def joint_trip_id(row: pd.Series) -> int:
    """
    Local function to generate a joint trip ID by concatenating the household ID and the joint trip number.

    Args:
        row (pd.Series): The trips table row

    Returns:
        int: the joint trip ID
    """
    return int(f'{row.hh_id}{row[JOINT_TRIPNUM_COL]:02d}')


# Generate a function that returns the disjoint set of a graph
def disjoint_set(edges: np.ndarray) -> dict:
    """
    This function returns the disjoint set of a graph of edges

    Args:
        edges (np.ndarray): The trip pairs to be checked for disjointedness

    Returns:
        dict: A dictionary of disjointed sets withere the key is the index ID of the trip and the value is the set ID (aka joint trip number)
    """
    
    parents = {}
    for i, edge in enumerate(edges, start=1):
        for x in edge:
            if x not in parents:
                parents[x] = i
    
    return parents

