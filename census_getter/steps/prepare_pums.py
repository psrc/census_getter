import pandas as pd
import numpy as np
from census_getter.util import Util


def get_filename(tablename, util):
    # Returns the filename for a given table from settings.yaml
    table_list = util.settings.get('pums_table_list', [])
    for table in table_list:
        if table['tablename'] == tablename:
            return table['filename']

def prepare_pums(util):
    """
    Combine and filter PUMS data for use with other data
    """

    pums_hh = pd.read_csv(f"{util.get_data_dir()}/{get_filename('pums_hh', util)}",low_memory=False)
    pums_person = pd.read_csv(f"{util.get_data_dir()}/{get_filename('pums_person', util)}",low_memory=False)
    puma_geog_lookup = pd.read_csv(f"{util.get_data_dir()}/{get_filename('puma_geog_lookup', util)}",low_memory=False)

    # Filter for records that exist only within the geo_cross_walk
    pums_hh = pums_hh[pums_hh['PUMA'].isin(puma_geog_lookup['PUMA'])]
    pums_person = pums_person[pums_person['PUMA'].isin(puma_geog_lookup['PUMA'])]

    # Filter for households without group quarters
    if util.settings['census_year'] > 2020:
        pums_hh = pums_hh[pums_hh['TYPEHUGQ'].isin([1])]
    else:
        pums_hh = pums_hh[pums_hh['TYPE'].isin([1,2])]

    # Filter for person/household matches
    pums_person = pums_person[pums_person['SERIALNO'].isin(pums_hh['SERIALNO'])]
    pums_hh = pums_hh[pums_hh['SERIALNO'].isin(pums_person['SERIALNO'])]
    pums_person.index = pums_person['SERIALNO']
    pums_hh.index = pums_hh['SERIALNO']

    # Generate unique household ID "hhnum"
    pums_hh['hhnum'] = [i+1 for i in range(len(pums_hh))]
    pums_person['hhnum'] = 0
    pums_person.update({'hhnum':pums_hh.hhnum})

    # Calculate household workers based on person records
    pums_person['is_worker'] = 0
    pums_person.loc[pums_person['ESR'].isin([1,2,4,5]), 'is_worker'] = 1
    worker_count = pums_person.groupby('hhnum').sum()[['is_worker']]
    pums_hh['worker_count'] = -99
    pums_hh.index = pums_hh.hhnum
    pums_hh.update({'worker_count': worker_count.is_worker})

    # Combine households with workers >= 3
    #pums_hh.loc[pums_hh['worker_count'] >= 3,'worker_count'] = 3

    # we are using 2021 5 year pums to have consistent PUMs geography (2010). 
    # adjust income to 2022. 
    pums_hh['HINCP'] = pums_hh.HINCP * (pums_hh.ADJINC/1000000)
    # adjust home value
    pums_hh['VALP'] = pums_hh['VALP'] * (pums_hh.ADJHSG/1000000)

    # add county_id
    if 'county_id' in puma_geog_lookup.columns:
        puma_cnty = puma_geog_lookup.groupby('PUMA')['county_id'].first().reset_index()
        pums_hh = pums_hh.merge(puma_cnty, on='PUMA', how='left')
        pums_person = pums_person.merge(puma_cnty, on='PUMA', how='left')

    # add occupation group codes
    occ_code_xwalk = {
        11: 1113, 13: 1113, 15: 1517, 17: 1517, 19: 19, 21: 21, 23: 23, 25: 25,
        27: 27, 29: 2931, 31: 2931, 33: 33, 35: 35, 37: 3739, 39: 3739, 41: 4143,
        43: 4143, 45: 45, 47: 47, 49: 49, 51: 51, 53: 53, 55: 55
    }
    pums_person['SOCP_2digit'] = pums_person['SOCP'].str[:2].astype(float)
    pums_person['occupation'] = np.where(pums_person['is_worker'] == 1, pums_person['SOCP_2digit'].map(occ_code_xwalk), 0)

    # add industry group codes
    ind_code_xwalk = {
        # agriculture, forestry, mining, construction
        11: 1, 21: 1, 23: 1, 
        # manufacturing, wholesale, transportation, warehousing, utilities
        22:2, 31: 2, 32: 2, 33: 2, 42: 2, 48: 2, 49: 2,
        # retail, accommodation, food services, arts, entertainment, other services
        44: 3, 45: 3, 72: 3, 71: 3, 81: 3,
        # information, finance, insurance, real estate, professional services
        51: 4, 52: 4, 53: 4, 54: 4, 55: 4, 56: 4,
        # government, education, health care, social assistance
        61: 5, 62: 5, 63: 5, 92: 5
    }
    pums_person['NAICSP_2digit'] = pums_person['NAICSP'].str[:2].replace({'3M': '31','4M':'41'}).astype(float)
    pums_person['aggr_industry'] = np.where(pums_person['is_worker'] == 1, pums_person['NAICSP_2digit'].map(ind_code_xwalk), 0)
    # set any government worker to industry 5 regardless of naics code
    pums_person.loc[pums_person['COW'].isin([3,4,5]), 'aggr_industry'] = 5

    util.save_table("seed_persons", pums_person)
    util.save_table("seed_households", pums_hh)

def run_step(context):
    print("Preparing PUMS data...")
    util = Util(settings_path=context['configs_dir'])
    prepare_pums(util)
    return context