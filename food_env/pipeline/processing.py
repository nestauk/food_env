def combinbe_hackney_and_city_of_london(
    indicator, region_lbl, hackney_lbl="Hackney", city_of_london_lbl="City of London"
):
    """Given a dataframe, combine rows for Hackney and City of London
    into one row.

    Args:
        indicator (df): indicator dataframe with rows for
            Hackney and City of London that need to be combined
        region_lbl (str): dataframe column name relating to region
        hackney_lbl (str): region label for Hackney,
            defaults to "Hackney"
        city_of_london_lbl (str): region label for City of London,
            defaults to "City of London"

    Returns:
        (df): indicator dataframe with Hackney and City of
            London rows combined
    """
    hackney_and_city_of_london_idx = indicator.last_valid_index() + 1
    hackney_idx = indicator.index[indicator[region_lbl] == hackney_lbl][0]
    city_of_london_idx = indicator.index[indicator[region_lbl] == city_of_london_lbl][0]

    indicator.loc[hackney_and_city_of_london_idx] = (
        indicator.loc[hackney_idx] + indicator.loc[city_of_london_idx]
    )
    indicator.at[
        hackney_and_city_of_london_idx, region_lbl
    ] = "Hackney and City of London"
    indicator.drop([hackney_idx, city_of_london_idx], axis=0, inplace=True)
    indicator.reset_index(drop=True, inplace=True)
    return indicator