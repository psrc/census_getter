import pandas as pd
from util.util import Util

util = Util()
settings = util.settings


def get_filename(tablename):
    # Returns the filename for a given table from settings.yaml
    table_list = settings.get('pums_table_list', [])
    for table in table_list:
        if table['tablename'] == tablename:
            return table['filename']

def prepare_pums():
    """
    Combine and filter PUMS data for use with other data
    """

    pums_hh = pd.read_csv(f"{util.get_data_dir()}/{get_filename('pums_hh')}",low_memory=False)
    pums_person = pd.read_csv(f"{util.get_data_dir()}/{get_filename('pums_person')}",low_memory=False)
    puma_geog_lookup = pd.read_csv(f"{util.get_data_dir()}/{get_filename('puma_geog_lookup')}",low_memory=False)

    # Filter for records that exist only within the geo_cross_walk
    pums_hh = pums_hh[pums_hh['PUMA'].isin(puma_geog_lookup['PUMA'])]
    pums_person = pums_person[pums_person['PUMA'].isin(puma_geog_lookup['PUMA'])]

    # Filter for households without group quarters
    if settings['census_year'] > 2020:
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

    # drop serialno to save memory
    pums_hh = pums_hh.drop(columns=['SERIALNO'])
    pums_person = pums_person.drop(columns=['SERIALNO'])
    
    util.save_table("seed_persons", pums_person)
    util.save_table("seed_households", pums_hh)

def run_step(context):
    print("Preparing PUMS data...")
    prepare_pums()
    return context