"""Data getters for the Public Health England.

Data Source: https://fingertips.phe.org.uk/
"""
import pandas as pd
from food_env import PROJECT_DIR

PHE = PROJECT_DIR / "inputs/public_health_england"


def get_all_mental_health_wellbeing():
    """Returns dataframe of all Children and Young People's
    Mental Health and Wellbeing indicators relating to
    'identification of need' for London
    Data source: https://fingertips.phe.org.uk/profile-group/mental-health/profile/cypmh/data#page/0/gid/1938133090/pat/6/par/E12000007/ati/102/iid/93587/age/221/sex/4/cat/-1/ctp/-1/yrr/1/cid/4/tbm/1
    """
    return pd.read_csv(
        PHE / "children_and_young_peoples_mental_health_and_wellbeing_london.csv"
    ).rename(str.lower, axis="columns")


def get_school_mental_health_needs():
    """Returns dataframe of areaname, count and denominator for
    school pupils with social, emotional and mental health needs
    """
    mhw = get_all_mental_health_wellbeing()
    return mhw[mhw["age"] == "School age"][["areaname", "count", "denominator"]].rename(
        columns={
            "count": "school_mental_health_needs_count",
            "denominator": "school_mental_health_needs_denominator",
        }
    )


def get_all_wider_determinants_of_health():
    """Returns dataframe of all Public Health Outcomes Framework
    indicators relating to 'Wider determinants of health' for London
    Data source: https://fingertips.phe.org.uk/profile/public-health-outcomes-framework/data#page/0/gid/1000041/pat/6/par/E12000007/ati/402/iid/93701/age/169/sex/4/cat/-1/ctp/-1/yrr/1/cid/4/tbm/1
    """
    return pd.read_csv(PHE / "wider_determinants_of_health_london.csv").rename(
        str.lower, axis="columns"
    )


def get_fuel_poverty():
    """Returns dataframe of areaname, count and denominator for
    people in fuel poverty
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[
        wdoh["indicator name"]
        == "B17 - Fuel poverty (low income, low energy efficiency methodology)"
    ][["areaname", "count", "denominator"]].rename(
        columns={"count": "fuel_pov_count", "denominator": "fuel_pov_denominator"}
    )


def get_violent_crime():
    """Returns dataframe of areaname, count and denominator for
    violent offences
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[
        wdoh["indicator name"]
        == "B12b - Violent crime - violence offences per 1,000 population"
    ][["areaname", "count", "denominator"]].rename(
        columns={
            "count": "violent_crime_count",
            "denominator": "violent_crime_denominator",
        }
    )


def get_youth_justice_system():
    """Returns dataframe of areaname, count and denominator for
    first time entrants to the youth justice system
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[
        wdoh["indicator name"]
        == "B04 - First time entrants to the youth justice system"
    ][["areaname", "count", "denominator"]].rename(
        columns={
            "count": "youth_justice_count",
            "denominator": "youth_justice_denominator",
        }
    )


def get_school_readiness():
    """Returns dataframe of areaname, count and denominator for
    children achieving at least the expected level of development
    in communication, language and literacy skills at the end of Reception
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[wdoh["indicator id"] == 93569][
        ["areaname", "count", "denominator"]
    ].rename(
        columns={
            "count": "school_readiness_count",
            "denominator": "school_readiness_denominator",
        }
    )


def get_high_night_time_transport_noise():
    """Returns dataframe of areaname, count and denominator for
    the population exposed to road, rail and air transport noise
    of 55 dB(A) or more during the night-time
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[wdoh["indicator id"] == 90358][
        ["areaname", "count", "denominator"]
    ].rename(
        columns={
            "count": "night_time_transport_noise_count",
            "denominator": "night_time_transport_noise_denominator",
        }
    )


def get_low_income():
    """Returns dataframe of areaname, count and denominator
    for children in absolute low income
    """
    wdoh = get_all_wider_determinants_of_health()
    return wdoh[wdoh["indicator id"] == 93701][
        ["areaname", "count", "denominator"]
    ].rename(
        columns={
            "count": "low_income_count",
            "denominator": "low_income_denominator",
        }
    )


def get_adult_loneliness():
    """Returns dataframe of areaname and percentage
    of adults who feel lonely often / always or
    some of the time
    Data Source: https://fingertips.phe.org.uk/search/lonely#page/3/gid/1/pat/6/par/E12000007/ati/102/are/E09000002/iid/93758/age/164/sex/4/cat/-1/ctp/-1/yrr/1/cid/4/tbm/1"""
    return (
        pd.read_csv(PHE / "adults_who_feel_lonely.csv", usecols=["AreaName", "Value"])
        .rename(str.lower, axis="columns")
        .dropna()
        .rename(columns={"value": "adult_loneliness_%"})
    )
