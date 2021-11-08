"""Data getters for Impact on Urban Health

Data source: https://guysandstthomas.communityinsight.org/dashboard/
"""
import pandas as pd
from food_env import PROJECT_DIR

UH = PROJECT_DIR / "inputs/impact_on_urban_health"


def get_food_vulnerability():
    """Returns dataframe of food vulnerability index
    score by geography. This was originally computed by the
    Red Cross as part of their covid vulerability index.
    See: https://docs.google.com/document/d/1aWpzgvLKGEF5Ay_xVps17nnbT1zIEki7RGIIJXL5APo/edit#heading=h.6576u7dtopmw
    Note: Hackney and City of London are not combined
    and food_vulnerability_index_score has already been
    computed, so it is hard to combine the two regions.
    """
    return (
        pd.read_csv(
            UH / "covid_vulnerability.csv",
            usecols=[0, 1, 5],
            header=0,
            names=["level", "areaname", "food_vulnerability_index_score"],
        )
        .query('level == "Uncategorised"')[
            ["areaname", "food_vulnerability_index_score"]
        ]
        .reset_index(drop=True)
    )
