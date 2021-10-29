# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from food_env.getters.department_work_pensions import get_low_income
from food_env.getters.office_national_statisics import get_population
from food_env.getters.department_health import get_childhood_obesity
from food_env.getters.public_health_england import get_school_mental_health_needs

# %%
# load and reformat low income data
li = get_low_income()
li.loc[217] = li.loc[87] + li.loc[86]
li.at[217, "local_authority"] = "Hackney and City of London"
li.drop([87, 86], axis=0, inplace=True)
li.reset_index(drop=True, inplace=True)
li = li[["local_authority", "2019/20"]]

# %%
# load and reformat child obesity data
owob = get_childhood_obesity()[
    ["ons_code", "area", "overweight_year_6", "obese_incl_severely_obese_year_6"]
].head(-10)
owob = owob[owob["overweight_year_6"] != "u"].reset_index(drop=True)
owob["overweight_and_obese_year_6"] = (
    owob["overweight_year_6"] + owob["obese_incl_severely_obese_year_6"]
)
owob = owob[["area", "overweight_and_obese_year_6"]]
# owob.sort_values(by=['overweight_and_obese_year_6'])

# %%
# load and reformat pupil mental health needs
mh = get_school_mental_health_needs()
mh.loc[490] = mh.loc[468] + mh.loc[463]
mh.at[490, "areaname"] = "Hackney and City of London"
mh.drop([468, 463], axis=0, inplace=True)
mh.reset_index(drop=True, inplace=True)
mh["pupil_mental_health_needs"] = mh["count"] / mh["denominator"] * 100
mh

# %%
# load and reformat population data and filter for london
pop = get_population()[["region_name", "geography", "all_ages"]].rename(
    columns={"all_ages": "population"}
)
ldn = pop[pop["geography"] == "London Borough"].drop(columns=["geography"])
ldn.loc[247] = ldn.loc[215] + ldn.loc[216]
ldn.at[247, "region_name"] = "Hackney and City of London"
ldn.drop([215, 216], axis=0, inplace=True)
ldn.reset_index(drop=True, inplace=True)

# %%
# merge low income with obesity datasets
li_owob = (
    pd.merge(
        left=owob, right=li, how="inner", left_on="area", right_on="local_authority"
    )
    .drop(columns=["area"])
    .rename(columns={"2019/20": "n_children_abs_low_income_families_2019-20"})
    .astype({"overweight_and_obese_year_6": "float"})
)
# merge low income with obesity datasets with population data
li_owob_ldn = (
    pd.merge(
        left=li_owob,
        right=ldn,
        how="inner",
        left_on="local_authority",
        right_on="region_name",
    )
    .drop(columns=["region_name"])
    .astype({"population": "float"})
)

# normalise low income with population
li_owob_ldn["n_children_abs_low_income_families_2019-20/population"] = (
    li_owob_ldn["n_children_abs_low_income_families_2019-20"]
    / li_owob_ldn["population"]
)

# merge low income with obesity datasets with population data with mental health data
li_owob_ldn_mh = (
    pd.merge(
        left=li_owob_ldn,
        right=mh,
        how="inner",
        left_on="local_authority",
        right_on="areaname",
    )
    .drop(columns=["count", "denominator", "areaname"])
    .astype({"pupil_mental_health_needs": "float"})
)

# %%
# check which local authorities are missing from merged dataset
missing = list(sorted(set(ldn.region_name) - set(li_owob.local_authority)))
missing
# local authorities are missing as numbers too small in Enfield/Wandsworth for the obesity data

# %%
# select relevant columns for correlations
for_corr = li_owob_ldn_mh[
    [
        "overweight_and_obese_year_6",
        "n_children_abs_low_income_families_2019-20/population",
        "pupil_mental_health_needs",
    ]
].rename(
    columns={
        "n_children_abs_low_income_families_2019-20/population": "children_low_income"
    }
)

# %%
# plot correlations
correlations = for_corr.corr()
correlations.style.background_gradient(cmap="Blues")

# %%
# low income "strongly" correlated with obesity
# mental health "very weakly" correlated with low income and obesity

# %%
