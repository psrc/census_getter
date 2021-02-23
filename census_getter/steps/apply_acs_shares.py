import logging
import os
import re

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import assign


from census_getter.util import setting, create_block_group_id
#from census_getter.util import create_block_group_id
from synthpop.census_helpers import Census

logger = logging.getLogger(__name__)

def read_spec(fname):
    cfg = pd.read_csv(fname, comment='#')
    
    # backfill description
    if 'Description' not in cfg.columns:
        cfg.description = ''

    cfg.target = cfg.target.str.strip()
    cfg.expression = cfg.expression.str.strip()

    return cfg

@inject.step()
def apply_acs_shares(settings, configs_dir):
    expression_file_path = os.path.join(configs_dir, settings['apply_acs_shares_expression_file'])
    spec = read_spec(expression_file_path)

    for input_table in settings['input_table_list']:
        tablename = input_table['tablename']
        create_block_group_id(tablename)

        inject.broadcast(cast=tablename, onto='combined_acs', cast_on = 'block_group_id', onto_on='block_group_id')
        
    input_table_list = [settings['input_table_list'][i]['tablename'] for i in range(len(settings['input_table_list']))]
    df = inject.merge_tables(target='combined_acs', tables=input_table_list + ['combined_acs'])
    inject.add_table("all_input_data", df)
    
    locals_d = {}

    results, trace_results, trace_assigned_locals = assign.assign_variables(spec, df, locals_d)

    results = results.round(0)

    inject.add_table("ofm_controls", results)