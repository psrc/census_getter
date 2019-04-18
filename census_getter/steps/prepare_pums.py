import logging
import os
import re

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import assign

from census_getter.util import setting, create_block_group_id
from synthpop.census_helpers import Census

logger = logging.getLogger(__name__)

@inject.step()
def prepare_pums(settings, configs_dir):
    """
    Combine and filter PUMS data for use with other data and populationsim format
    """

    pums_hh = inject.get_table('pums_hh').to_frame()
    pums_person = inject.get_table('pums_person').to_frame()

    # Add Census geography to PUMA
    puma_geog_lookup = inject.get_table('puma_geog_lookup').to_frame()

    # Filter for records that exist only within the geo_cross_walk
    pums_hh = pums_hh[pums_hh['PUMA'].isin(puma_geog_lookup['PUMA'])]
    pums_person = pums_person[pums_person['PUMA'].isin(puma_geog_lookup['PUMA'])]

    # Filter for households without group quarters
    pums_hh = pums_hh[pums_hh['TYPE'].isin([1,2])]

    # Filter for person/household matches
    pums_person = pums_person[pums_person['SERIALNO'].isin(pums_hh['SERIALNO'])]
    pums_hh = pums_hh[pums_hh['SERIALNO'].isin(pums_person['SERIALNO'])]
    pums_person.index = pums_person['SERIALNO']
    pums_hh.index = pums_hh['SERIALNO']

    # Generate unique household ID "hhnum"
    pums_hh['hhnum'] = [i+1 for i in xrange(len(pums_hh))]
    pums_person['hhnum'] = 0
    pums_person.hhnum.update(pums_hh.hhnum)

    # Calculate household workers based on person records
    pums_person['is_worker'] = 0
    pums_person.loc[pums_person['ESR'].isin([1,2,4,5]), 'is_worker'] = 1
    worker_count = pums_person.groupby('hhnum').sum()[['is_worker']]
    pums_hh['worker_count'] = -99
    pums_hh.index = pums_hh.hhnum
    pums_hh.worker_count.update(worker_count.is_worker)

    # Combine households with workers >= 3
    pums_hh.loc[pums_hh['worker_count'] >= 3,'worker_count'] = 3

    inject.add_table("seed_persons", pums_person)
    inject.add_table("seed_households", pums_hh)