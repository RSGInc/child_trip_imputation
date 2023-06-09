def impute_nonproxy(persons_df, trips_df):
    """
    Impute trips from non-proxy household members
    """
    # Get trips where household member 2 is present, but for which no trip exists
    
    # assert person.shape[0] == 1, 'Person must be a single row'
    
    # col = f'hh_member_{person.iloc[0].person_num}'
    # is_hh_member = household_day_trips[col] == 1
    # is_not_existing = ~household_day_trips.index.isin(person_tour_trips.index)
    
    # household_day_trips[is_not_existing & is_hh_member]
                                    
    # list(household_day_trips.columns)
    
    # household_day_trips.filter(regex='hh_member_')
    
    return trips_df