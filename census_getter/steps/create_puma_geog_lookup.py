import pandas as pd
import geopandas as gpd
import requests
import re

from census_getter.util import Util


def get_census_geography_year(pums_year):
    # determins which PUMA geography year to use based on PUMS year
    if pums_year >= 2023 and pums_year <= 2032:
        puma_geog_year = 2020
    elif pums_year == 2022:
        raise ValueError("PUMS data for 2022 is split between 2010 and 2020 PUMA geographies. Please choose either 2021 or 2023 for pums_year.")
    elif pums_year >=2016 and pums_year <= 2021:
        puma_geog_year = 2010
    elif pums_year < 2016:
        raise ValueError("PUMS is only setup for 2016 onwards in this repo to work with either 2010 or 2020 PUMA geographies. Pre-2016 PUMS data includes PUMA 2000 geographies.")
    else:
        raise ValueError("PUMS year out of range.")
    return puma_geog_year

def get_most_recent_tiger_year():
    # Determine the most recent TIGER year available by scraping the TIGER directory
    # Get the list of folders from the TIGER directory listing
    response = requests.get("https://www2.census.gov/geo/tiger/")
    # Use regex to find all folder names like TIGERYYYY in the HTML
    folders = re.findall(r'TIGER\d{4}/', response.text)
    folders = [folder.rstrip('/') for folder in folders]
    folders = list(set(folders))  # Remove duplicates if any

    # Find the most recent TIGER folder (e.g., TIGER2025)
    most_recent_year = max(int(folder[5:]) for folder in folders)
    return most_recent_year

def create_county_id(counties):
    return [int('53' + county) for county in counties]

def create_puma_geog_lookup(util):
    state = util.settings['state']
    counties = util.settings['counties']
    county_ids = create_county_id(counties)
    pums_year = util.settings['pums_year']
    
    # setup the url to download shapefiles from TIGERweb
    puma_geog_year = get_census_geography_year(pums_year)
    most_recent_year = get_most_recent_tiger_year()
    most_recent_folder = f"TIGER{most_recent_year}"
    puma_geog_year_two_digit = str(puma_geog_year)[-2:]
    geoid = 'GEOID' + puma_geog_year_two_digit

    # download PUMAs
    puma_url = f'https://www2.census.gov/geo/tiger/{most_recent_folder}/PUMA{puma_geog_year_two_digit}/tl_{most_recent_year}_{str(state)}_puma{puma_geog_year_two_digit}.zip'
    puma = (
        gpd.read_file(puma_url)
        .assign(puma_id = lambda df: df[geoid].astype(int))
        [['puma_id', 'geometry']]
    )

    # download block groups
    geog_url = f'https://www2.census.gov/geo/tiger/{most_recent_folder}/BG/tl_{most_recent_year}_{str(state)}_bg.zip'
    blk_grp = (
        gpd.read_file(geog_url)
        .assign(
            block_group_id = lambda df: df['GEOID'].astype('int64'),
            county_id = lambda df: df['GEOID'].str[:5].astype(int)
        )
        .query('county_id.isin(@county_ids)')
        [['block_group_id','geometry']]
    )

    # spatial join block group centroids to pumas
    blk_grp.geometry = blk_grp.representative_point()
    blk_grp_pts = blk_grp.sjoin(puma, how='left')

    # create PUMA column without state code prefix
    blk_grp_pts['PUMA'] = blk_grp_pts['puma_id'].astype(str).str[2:].astype(int)

    # create region column
    blk_grp_pts['region'] = 1

    # save to csv in data directory
    blk_grp_pts[['block_group_id', 'puma_id','PUMA','region']].to_csv(util.get_data_dir() + '/puma_geog_lookup.csv', index=False)


def run_step(context):
    # pypyr step to run load_data.py
    print("Creating PUMA <-> block group Lookup table...")
    util = Util(settings_path=context['configs_dir'])
    create_puma_geog_lookup(util)
    return context