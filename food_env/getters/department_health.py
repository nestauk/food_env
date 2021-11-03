"""Data getters for the Department of Health.

Data source: https://data.london.gov.uk/dataset/prevalence-childhood-obesity-borough
"""
import pandas as pd
from food_env import PROJECT_DIR

DOH = PROJECT_DIR / "inputs/department_of_health"


def get_childhood_obesity():
    """Returns dataframe of childhood weights for
    reception and year 6 by geography for 2019/2020
    Note: Hackney includes City of London"""
    cw = (
        pd.read_excel(
            DOH / "childhood_obesity_by_borough.xlsx",
            sheet_name="2019-20",
            header=2,
            usecols=[0, 1, 2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 33],
        )
        .dropna(axis=0, how="all")
        .reset_index(drop=True)
        .rename(
            columns={
                "ONS Code": "ons_code",
                "Area": "areaname",
                "Prevalence (%)": "underweight_reception",
                "Prevalence (%).1": "underweight_year_6",
                "Prevalence (%).2": "healthyweight_reception",
                "Prevalence (%).3": "healthyweight_year_6",
                "Prevalence (%).4": "overweight_reception",
                "Prevalence (%).5": "overweight_year_6",
                "Prevalence (%).6": "obese_incl_severely_obese_reception",
                "Prevalence (%).7": "obese_incl_severely_obese_year_6",
                "Prevalence (%).8": "severely_obese_reception",
                "Prevalence (%).9": "severely_obese_year_6",
                "Reception": "n_children_reception",
                "Year 6": "n_children_year_6",
            }
        )
    )
    cw.at[10, "areaname"] = "Hackney and City of London"
    return cw
