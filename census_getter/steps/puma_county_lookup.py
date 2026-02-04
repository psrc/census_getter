import pandas as pd
import geopandas as gpd

from census_getter.util import Util


#------------------------------
# Create PUMA to County Lookup
#------------------------------
# This uses the PUMA centroids to spatially join to counties
# to create a lookup table of PUMA to County relationships.
# This happens to work well for PSRC because no PUMAs cross county
# lines. If that were to change, a different method would be needed.

def create_puma_county_lookup(util):
    # create county geoid list
    state = util.settings['state']
    counties = util.settings['counties']
    county_geoids_str = [f'{state}{str(c).zfill(3)}' for c in counties]

    # download county shapefile from tigerweb
    cnty = (
        gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip')
        .query('GEOID.isin(@county_geoids_str)')
        .assign(GEOID =lambda df: df['GEOID'].astype(int))
        .rename(columns={'GEOID':'county_id'})
        [['county_id', 'geometry']]
    )

    # download 2010 puma shapefile from tigerweb
    puma10 = (
        gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2021/PUMA/tl_2021_53_puma10.zip')
        .assign(puma10_id = lambda df: df['GEOID10'].astype(int))
        .rename(columns={'PUMACE10':'PUMA'})
        [['PUMA','puma10_id', 'geometry']]
    )

    # get centroids of pumas
    puma10.geometry = puma10.representative_point()

    # spatial join pumas to counties
    puma_cnty = gpd.sjoin(puma10, cnty, how='inner', predicate='within').drop(columns='index_right')

    # create region column
    puma_cnty['region'] = 1

    # save to csv in data directory
    puma_cnty[['PUMA','puma10_id','county_id','region']].to_csv(util.get_data_dir() + '/puma_geog_lookup_cnty.csv', index=False)


def run_step(context):
    # pypyr step to run load_data.py
    print("Creating PUMA <-> County Lookup table...")
    util = Util(settings_path=context['configs_dir'])
    create_puma_county_lookup(util)
    return context