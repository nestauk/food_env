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
from food_env.getters.department_health import get_childhood_obesity
from food_env.getters.public_health_england import (
    get_school_mental_health_needs,
    get_low_income,
    get_school_readiness,
    get_fuel_poverty,
    get_high_night_time_transport_noise,
    get_violent_crime,
    get_youth_justice_system,
)
from food_env.pipeline.processing import (
    combine_hackney_and_city_of_london,
    add_percentage,
)

# %%
# load and reformat low income data
li = get_low_income()
li = combine_hackney_and_city_of_london(indicator=li, region_lbl="areaname")
li = add_percentage(
    indicator=li,
    new_percent_col="low_income_%",
    numerator_col="low_income_count",
    denominator_col="low_income_denominator",
)

# %%
# load and reformat child obesity data
owob = get_childhood_obesity()[
    ["ons_code", "areaname", "overweight_year_6", "obese_incl_severely_obese_year_6"]
].head(-10)
owob = owob[owob["overweight_year_6"] != "u"].reset_index(drop=True)
owob["overweight_and_obese_year_6"] = (
    owob["overweight_year_6"] + owob["obese_incl_severely_obese_year_6"]
)
owob = owob[
    [
        "areaname",
        "overweight_and_obese_year_6",
        "overweight_year_6",
        "obese_incl_severely_obese_year_6",
    ]
].astype(
    {
        "overweight_and_obese_year_6": "float",
        "overweight_year_6": "float",
        "obese_incl_severely_obese_year_6": "float",
    }
)
# owob.sort_values(by=['overweight_and_obese_year_6'])

# %%
# load and reformat pupil mental health needs data
mh = get_school_mental_health_needs()
mh = combine_hackney_and_city_of_london(indicator=mh, region_lbl="areaname")
mh = add_percentage(
    indicator=mh,
    new_percent_col="pupil_mental_health_needs_%",
    numerator_col="school_mental_health_needs_count",
    denominator_col="school_mental_health_needs_denominator",
)

# %%
# load and reformat school readiness data
sr = get_school_readiness()
sr = combine_hackney_and_city_of_london(indicator=sr, region_lbl="areaname")
sr = add_percentage(
    indicator=sr,
    new_percent_col="school_readiness_%",
    numerator_col="school_readiness_count",
    denominator_col="school_readiness_denominator",
)

# %%
# load and reformat fuel poverty data
fp = get_fuel_poverty()
fp = combine_hackney_and_city_of_london(indicator=fp, region_lbl="areaname")
fp = add_percentage(
    indicator=fp,
    new_percent_col="fuel_pov_%",
    numerator_col="fuel_pov_count",
    denominator_col="fuel_pov_denominator",
)

# %%
# load and reformat high night time transport noise data
tn = get_high_night_time_transport_noise()
tn = combine_hackney_and_city_of_london(indicator=tn, region_lbl="areaname")
tn = add_percentage(
    indicator=tn,
    new_percent_col="night_time_transport_noise_%",
    numerator_col="night_time_transport_noise_count",
    denominator_col="night_time_transport_noise_denominator",
)

# %%
# load and reformat violent crime data
vc = get_violent_crime()
vc = combine_hackney_and_city_of_london(indicator=vc, region_lbl="areaname")
# add_percentage here will not calc a % but will give rate per 100 people
vc = add_percentage(
    indicator=vc,
    new_percent_col="violent_crime_per_100_people",
    numerator_col="violent_crime_count",
    denominator_col="violent_crime_denominator",
)

# %%
# load and reformat entrants into youth justice system
yj = get_youth_justice_system().fillna(0)
yj = combine_hackney_and_city_of_london(indicator=yj, region_lbl="areaname")
# add_percentage here will not calc a % but will give rate per 100 people
yj = add_percentage(
    indicator=yj,
    new_percent_col="first_time_entrants_to_the_youth_justice_system_per_100_people",
    numerator_col="youth_justice_count",
    denominator_col="youth_justice_denominator",
)

# %%
# merge all datasets
combined_datasets = (
    owob.merge(li, on="areaname")
    .merge(mh, on="areaname")
    .merge(sr, on="areaname")
    .merge(fp, on="areaname")
    .merge(tn, on="areaname")
    .merge(vc, on="areaname")
    .merge(yj, on="areaname")
)

# %%
# # check which local authorities are missing from merged dataset
# missing = list(sorted(set(ldn.region_name) - set(li_owob.local_authority)))
# missing
# # local authorities are missing as numbers too small in Enfield/Wandsworth for the obesity data

# %%
# rename columns for correlation table
for_corr = combined_datasets.rename(
    columns={
        "low_income_%": "children_low_income",
        "pupil_mental_health_needs_%": "pupil_mental_health_needs",
        "school_readiness_%": "school_readiness",
        "fuel_pov_%": "fuel_poverty",
        "night_time_transport_noise_%": "night_time_transport_noise",
        "violent_crime_per_100_people": "violent_crime",
        "first_time_entrants_to_the_youth_justice_system_per_100_people": "youth_justice_system",
    }
)

# %%
# plot correlations
correlations = for_corr.corr()
correlations.style.background_gradient(cmap="Blues")

# %%
