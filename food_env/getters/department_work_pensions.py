"""Data getters for Department for Work
and Pensions data.

Data source: https://stat-xplore.dwp.gov.uk/webapi/jsf/dataCatalogueExplorer.xhtml
"""

import pandas as pd
from food_env import PROJECT_DIR

DWP = PROJECT_DIR / "inputs/dwp"


def get_low_income():
    """Returns dataframe of counts of children in absolute
    low income families by local authority by financial year"""
    return (
        pd.read_csv(
            DWP / "absolute_low_income_local_authority_by_financial_year.csv",
            header=6,
            usecols=["Year", "2015/16", "2016/17", "2017/18", "2018/19", "2019/20 (p)"],
        )
        .tail(-1)
        .head(-12)
        .rename(columns={"Year": "local_authority", "2019/20 (p)": "2019/20"})
        .astype(
            {
                "2015/16": "int32",
                "2016/17": "int32",
                "2017/18": "int32",
                "2018/19": "int32",
                "2019/20": "int32",
            }
        )
    )
