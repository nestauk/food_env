"""Data getters for the Office of National Statistics.

Data source: https://www.ons.gov.uk/
"""
import pandas as pd
from food_env import PROJECT_DIR

ONS = PROJECT_DIR / "inputs/ons"


def get_population():
    """Returns dataframe of ONS population
    estimates by geography and age for 2020
    """
    return (
        pd.read_excel(
            ONS / "ukpopestimatesmid2020on2021geography.xls",
            sheet_name="MYE2 - Persons",
            header=7,
        )
        .rename(columns={"Name": "region_name", "All ages": "all_ages"})
        .rename(str.lower, axis="columns")
    )


def get_happiness():
    """Returns dataframe for ONS percentage with high levels
    of happiness for London local authorities for 2018/2019
    """
    return pd.read_excel(
        ONS / "personal_wellbeing_borough.xlsx",
        sheet_name="Happy",
        usecols=[1, 39],
        skiprows=2,
        skipfooter=17,
        names=["areaname", "high_levels_happiness_%"],
    )


def get_satisfaction():
    """Returns dataframe for ONS percentage with high levels
    of satisfaction for London local authorities for 2018/2019
    """
    return pd.read_excel(
        ONS / "personal_wellbeing_borough.xlsx",
        sheet_name="Life Satisfaction",
        usecols=[1, 39],
        skiprows=2,
        skipfooter=17,
        names=["areaname", "high_levels_satisfaction_%"],
    )
