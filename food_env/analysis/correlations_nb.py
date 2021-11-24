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
    get_adult_loneliness,
    get_adult_obesity,
    get_fast_food,
)
from food_env.getters.office_national_statistics import get_happiness, get_satisfaction
from food_env.getters.urban_health import get_food_vulnerability
from food_env.pipeline.processing import (
    combine_hackney_and_city_of_london,
    add_percentage,
    combine_hackney_and_city_of_london_and_add_percentage,
)
import dataframe_image as dfi

# %%
# load and reformat low income families data
li = combine_hackney_and_city_of_london_and_add_percentage(
    get_low_income(),
    new_percent_col="low_income_%",
    numerator_col="low_income_count",
    denominator_col="low_income_denominator",
)

# %%
# load and reformat child obesity data
owob = (
    get_childhood_obesity()
    .head(-10)  # lose higher level regions
    .query('overweight_year_6 != "u"')  # filter out supressed records
    .query('obese_incl_severely_obese_year_6 != "u"')
    .reset_index(drop=True)
    .assign(
        overweight_and_obese_year_6=lambda x: x["overweight_year_6"]
        + x["obese_incl_severely_obese_year_6"]
    )[
        [
            "areaname",
            "overweight_and_obese_year_6",
            "overweight_year_6",
            "obese_incl_severely_obese_year_6",
        ]
    ]
    .astype(
        {
            "overweight_and_obese_year_6": "float",
            "overweight_year_6": "float",
            "obese_incl_severely_obese_year_6": "float",
        }
    )
)
# owob.sort_values(by=['overweight_and_obese_year_6'])

# %%
# load and reformat school mental health data
mh = combine_hackney_and_city_of_london_and_add_percentage(
    get_school_mental_health_needs(),
    new_percent_col="pupil_mental_health_needs_%",
    numerator_col="school_mental_health_needs_count",
    denominator_col="school_mental_health_needs_denominator",
)

# %%
# load and reformat school readiness data
sr = combine_hackney_and_city_of_london_and_add_percentage(
    get_school_readiness(),
    new_percent_col="school_readiness_%",
    numerator_col="school_readiness_count",
    denominator_col="school_readiness_denominator",
)

# %%
# load and reformat fuel poverty data
fp = combine_hackney_and_city_of_london_and_add_percentage(
    get_fuel_poverty(),
    new_percent_col="fuel_pov_%",
    numerator_col="fuel_pov_count",
    denominator_col="fuel_pov_denominator",
)

# %%
# load and reformat high night time transport noise data
tn = combine_hackney_and_city_of_london_and_add_percentage(
    get_high_night_time_transport_noise(),
    new_percent_col="night_time_transport_noise_%",
    numerator_col="night_time_transport_noise_count",
    denominator_col="night_time_transport_noise_denominator",
)

# %%
# load and reformat violent crime data
# add_percentage here will not calc a % but will give rate per 100 people
vc = combine_hackney_and_city_of_london_and_add_percentage(
    get_violent_crime(),
    new_percent_col="violent_crime_per_100_people",
    numerator_col="violent_crime_count",
    denominator_col="violent_crime_denominator",
)

# %%
# load and reformat entrants into youth justice system
# add_percentage here will not calc a % but will give rate per 100 people
yj = combine_hackney_and_city_of_london_and_add_percentage(
    get_youth_justice_system().fillna(0),
    new_percent_col="first_time_entrants_to_the_youth_justice_system_per_100_people",
    numerator_col="youth_justice_count",
    denominator_col="youth_justice_denominator",
)

# %%
# load and reformat food vulnerability, loneliness, satisfaction, happiness and fast food
fv = get_food_vulnerability().replace("Hackney", "Hackney and City of London")
al = get_adult_loneliness().replace("Hackney", "Hackney and City of London")
sat = (
    get_satisfaction()
    .query('areaname != "City of London"')
    .replace("Hackney", "Hackney and City of London")
)
hap = (
    get_happiness()
    .query('areaname != "City of London"')
    .replace("Hackney", "Hackney and City of London")
)
ff = (
    get_fast_food()
    .query('areaname != "City of London*"')
    .replace("Hackney", "Hackney and City of London")
)
# The other datasets have Hackney and City of London,
# but here Hackney and City of London are not combined
# and the values are already computed or City of London is
# suppressed. Therefore Hackney and City of London
# cannot be combined for these indicators...
# can rename Hackney to 'Hackney and City of London'
# and value will be slightly wrong
# or do nothing and Hsckney and City of London will be excluded
# from the correlations


# %%
# load and refroamt adult obesity data
ao = combine_hackney_and_city_of_london_and_add_percentage(
    get_adult_obesity(),
    new_percent_col="adults_overweight_or_obese_%",
    numerator_col="overweight_or_obese_count",
    denominator_col="overweight_or_obese_denominator",
)

# %%
# merge all datasets
dfs = [owob, li, mh, sr, fp, tn, vc, yj, fv, al, sat, hap, ao, ff]
for df in dfs:
    df.set_index("areaname", inplace=True)
combined_datasets = (
    pd.concat(dfs, axis=1, sort=False, join="inner")
    .reset_index()
    .rename(columns={"index": "areaname"})
)

# %%
# check which local authorities are missing from merged dataset
# missing = list(sorted(set(combined_datasets1.areaname) - set(combined_datasets2.areaname)))
# missing

# %%
# rename columns for correlation table
for_corr = combined_datasets.rename(
    columns={
        "low_income_%": "child_low_income_families",
        "pupil_mental_health_needs_%": "pupil_mental_health_needs",
        "school_readiness_%": "school_readiness",
        "fuel_pov_%": "fuel_poverty",
        "night_time_transport_noise_%": "night_time_transport_noise",
        "violent_crime_per_100_people": "violent_crime",
        "first_time_entrants_to_the_youth_justice_system_per_100_people": "youth_justice_system",
        "food_vulnerability_index_score": "food_vulnerability",
        "adult_loneliness_%": "adult_loneliness",
        "high_levels_satisfaction_%": "adult_satisfaction",
        "high_levels_happiness_%": "adult_happiness",
        "adults_overweight_or_obese_%": "adults_overweight_or_obese",
        "fast_food_rate_per_100000_pop": "fast_food_restaurants",
    }
)

# %%
# plot correlations
correlations = for_corr.corr().style.background_gradient(cmap="coolwarm")
dfi.export(correlations, "correlations.png")
correlations
