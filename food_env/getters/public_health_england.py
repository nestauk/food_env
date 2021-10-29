"""Data getters for the Public Health England.

Data source: https://fingertips.phe.org.uk/profile-group/mental-health/profile/cypmh
"""
import pandas as pd
from food_env import PROJECT_DIR

PHE = PROJECT_DIR / "inputs/public_health_england"


def get_all_mental_health_wellbeing():
    """Returns dataframe of all Children and Young People's
    Mental Health and Wellbeingindicators relating to
    'identification of need' for London"""
    return pd.read_csv(
        PHE / "children_and_young_peoples_mental_health_and_wellbeing_london.csv"
    ).rename(str.lower, axis="columns")


def get_school_mental_health_needs():
    """Returns dataframe for School pupils with social,
    emotional and mental health needs"""
    mhw = get_all_mental_health_wellbeing()
    return mhw[mhw["age"] == "School age"][["areaname", "count", "denominator"]]
