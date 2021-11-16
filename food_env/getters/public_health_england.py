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
    for London local authorities for 2020
    """
    return (
        get_all_mental_health_wellbeing()
        .query("age == 'School age'")[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "school_mental_health_needs_count",
                "denominator": "school_mental_health_needs_denominator",
            }
        )
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
    people in fuel poverty for 2019
    """

    return (
        get_all_wider_determinants_of_health()
        .query(
            "`indicator name` == 'B17 - Fuel poverty (low income, low energy efficiency methodology)'"
        )[["areaname", "count", "denominator"]]
        .rename(
            columns={"count": "fuel_pov_count", "denominator": "fuel_pov_denominator"}
        )
    )


def get_violent_crime():
    """Returns dataframe of areaname, count and denominator for
    violent offences for 2020/21
    """
    return (
        get_all_wider_determinants_of_health()
        .query(
            "`indicator name` == 'B12b - Violent crime - violence offences per 1,000 population'"
        )[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "violent_crime_count",
                "denominator": "violent_crime_denominator",
            }
        )
    )


def get_youth_justice_system():
    """Returns dataframe of areaname, count and denominator for
    first time entrants to the youth justice system for 2020
    """
    return (
        get_all_wider_determinants_of_health()
        .query(
            "`indicator name` == 'B04 - First time entrants to the youth justice system'"
        )[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "youth_justice_count",
                "denominator": "youth_justice_denominator",
            }
        )
        .fillna(0)
    )


def get_school_readiness():
    """Returns dataframe of areaname, count and denominator for
    children achieving at least the expected level of development
    in communication, language and literacy skills at the end of Reception
    for 2018/19
    """
    return (
        get_all_wider_determinants_of_health()
        .query("`indicator id` == 93569")[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "school_readiness_count",
                "denominator": "school_readiness_denominator",
            }
        )
    )


def get_high_night_time_transport_noise():
    """Returns dataframe of areaname, count and denominator for
    the population exposed to road, rail and air transport noise
    of 55 dB(A) or more during the night-time for 2016
    """
    return (
        get_all_wider_determinants_of_health()
        .query("`indicator id` == 90358")[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "night_time_transport_noise_count",
                "denominator": "night_time_transport_noise_denominator",
            }
        )
    )


def get_low_income():
    """Returns dataframe of areaname, count and denominator
    for children in absolute low income 2019/20
    """
    return (
        get_all_wider_determinants_of_health()
        .query("`indicator id` == 93701")[["areaname", "count", "denominator"]]
        .rename(
            columns={
                "count": "low_income_count",
                "denominator": "low_income_denominator",
            }
        )
    )


def get_adult_loneliness():
    """Returns dataframe of areaname and percentage
    of adults who feel lonely often / always or
    some of the time for 2019/20
    Data Source: https://fingertips.phe.org.uk/search/lonely#page/3/gid/1/pat/6/par/E12000007/ati/102/are/E09000002/iid/93758/age/164/sex/4/cat/-1/ctp/-1/yrr/1/cid/4/tbm/1"""
    return (
        pd.read_csv(PHE / "adults_who_feel_lonely.csv", usecols=["AreaName", "Value"])
        .rename(str.lower, axis="columns")
        .dropna()
        .rename(columns={"value": "adult_loneliness_%"})
    )


def get_adult_obesity():
    """Returns dataframe of areaname, count and denominator
    for adults (aged 18+) classified as overweight or obese
    for 2019/20
    Data Source: https://fingertips.phe.org.uk/search/weight#page/3/gid/1/pat/6/par/E12000007/ati/102/are/E09000002/iid/93088/age/168/sex/4/cat/-1/ctp/-1/yrr/1/cid/4/tbm/1"""
    return (
        pd.read_csv(
            PHE / "adult_obesity.csv", usecols=["AreaName", "Value", "Denominator"]
        )
        .rename(str.lower, axis="columns")
        .assign(count=lambda x: x.value / 100 * x.denominator)
        .rename(
            columns={
                "count": "overweight_or_obese_count",
                "denominator": "overweight_or_obese_denominator",
            }
        )
    )[["areaname", "overweight_or_obese_denominator", "overweight_or_obese_count"]]


def get_fast_food():
    """Returns dataframe of areaname and fast food restaurant
    rate per 100,000 population on 31/12/2017
    Data Source: https://www.gov.uk/government/publications/fast-food-outlets-density-by-local-authority-in-england
    """
    return (
        pd.read_excel(
            PHE / "fast_food_metadata.xlsx",
            sheet_name="Local Authority Data",
            header=3,
            usecols=[1, 3, 4, 5],
        )
        .query("`PHE Centre` == 'London'")
        .drop(columns=["PHE Centre", "Count of outlets"])
        .reset_index(drop=True)
        .rename(
            columns={
                "LA name": "areaname",
                "Rate per 100,000 population": "fast_food_rate_per_100000_pop",
            }
        )
    )
