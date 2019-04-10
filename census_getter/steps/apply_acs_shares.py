import logging
import os
import re

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import assign


from census_getter.util import setting
#from census_getter.util import create_block_group_id
from synthpop.census_helpers import Census

logger = logging.getLogger(__name__)

#def block_group_id(my_table):
#    t = inject.get_table('my_table')
#    df = my_table.to_frame(columns=['state', 'county', 'tract', 'block group'])
#    for col in df.columns:
#        df[col] = df[col].astype('int').astype('str')
#    s = df['state']+df['county']+df['tract']+df['block group']
#    inject.add_column(my_table, 'block_group_id', s)
def create_block_group_id(my_table):
    t = inject.get_table(my_table)
    df = t.to_frame(columns=['state', 'county', 'tract', 'block group'])
    for col in df.columns:
        df[col] = df[col].astype('int').astype('str')
    s = df['state']+df['county']+df['tract']+df['block group']
    inject.add_column(my_table, 'block_group_id', s)

def read_spec(fname):
    cfg = pd.read_csv(fname, comment='#')
    
    # backfill description
    if 'Description' not in cfg.columns:
        cfg.description = ''

    cfg.target = cfg.target.str.strip()
    cfg.expression = cfg.expression.str.strip()

    return cfg
    
    #def _apply_external_control_totals(self, join_cols, )

@inject.step()
def apply_acs_shares(settings, configs_dir):
    expression_file_path = os.path.join(configs_dir, settings['apply_acs_shares_expression_file'])
    spec = read_spec(expression_file_path)

    create_block_group_id('ofm_control_totals')
    inject.broadcast(cast='ofm_control_totals', onto='combined_acs', cast_on = 'block_group_id', onto_on='block_group_id')
    inject.merge_tables(target='combined_acs', tables=['ofm_control_totals', 'combined_acs'])
    df = inject.merge_tables(target='combined_acs', tables=['ofm_control_totals', 'combined_acs'])
    locals_d = {
    }

    #if constants is not None:
    #    locals_d.update(constants)

    results, trace_results, trace_assigned_locals = assign.assign_variables(spec, df, locals_d)
    #ofm_df = pd.DataFrame(index=df.index)
    #for column in results.columns:
    #    data = np.asanyarray(results[column])
    #    data.shape = (zone_count, zone_count)
    #    accessibility_df[column] = np.log(np.sum(data, axis=1) + 1)
    #ofm_df.reset_index(inplace = True)
    # - write table to pipeline
    inject.add_table("ofm_controls", results)

