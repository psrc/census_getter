
import logging
import os
import re

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline

from census_getter.util import setting
from census_getter.census_helpers import Census

logger = logging.getLogger(__name__)

def get_acs_data(key, spec, state, county, census_year, tract=None):
        c = c = Census(os.environ[key])

        hh_bg_columns = get_column_names('block_group', 'household', spec)
        hh_tract_columns = get_column_names('tract', 'household', spec)

        if len([x for x in hh_bg_columns if x in hh_tract_columns]) > 0:
            raise RuntimeError("The same acs column is being used as block group and tract. Please check expression file.")

        h_acs = c.block_group_and_tract_query(
            hh_bg_columns, hh_tract_columns, state, county,
            merge_columns=['tract', 'county', 'state'],
            block_group_size_attr="B11001_001E",
            tract_size_attr="B08201_001E",
            tract=tract,
            year=census_year)
        
        pers_bg_columns = get_column_names('block_group', 'person', spec)
        pers_tract_columns = get_column_names('tract', 'person', spec)

        if len([x for x in pers_bg_columns if x in pers_tract_columns]) > 0:
            raise RuntimeError("The same acs column is being used as block group and tract. Please check expression file.")

        p_acs = c.block_group_and_tract_query(
            pers_bg_columns, pers_tract_columns, state, county,
            merge_columns=['tract', 'county', 'state'],
            block_group_size_attr='B01001_001E',
            tract_size_attr='B01003_001E',
            tract=tract,
            year = census_year)

        all_acs = h_acs.merge(p_acs, how = 'left', on = ['state', 'county', 'tract', 'block group'])
        
        return all_acs

def create_controls(spec):
        locals_d = {'df' : inject.get_table('all_acs').to_frame()}
        
        le = []
        
        for e in zip(spec.geog, spec.target,
                     spec.expression):
            geog, target, expression = e

            values = to_series(eval(expression, globals(), locals_d), target=target)
            le.append((target, values))
       
        variables = []
        seen = set()
        for statement in reversed(le):
            # statement is a tuple (<target_name>, <eval results in pandas.Series>)
            target_name = statement[0]
            if target_name not in seen:
                variables.insert(0, statement)
                seen.add(target_name)

         # DataFrame from list of tuples [<target_name>, <eval results>), ...]
        variables = pd.DataFrame.from_items(variables)
        variables = variables.merge(locals_d['df'][['state','county', 'tract', 'block group']], how='left', left_index = True, right_index = True)
        return variables


def read_spec(fname):
    cfg = pd.read_csv(fname, comment='#')
    
    # backfill description
    if 'description' not in cfg.columns:
        cfg.description = ''

        cfg.target = cfg.target.str.strip()
        cfg.expression = cfg.expression.str.strip()

        return cfg
    
    #def _apply_external_control_totals(self, join_cols, )

def get_column_names(geog, type, spec):
        expression_list = spec[(spec.geog==geog) & (spec.type==type)].expression.tolist()
        column_list = []
        for s in expression_list:
            res = re.findall(r"[^[]*\[([^]]*)\]", s)
            for item in res:
                 item = item .replace("'", '')
                 column_list.append(item)
        return list(set(column_list))
       
        

def to_series(x, target=None):
        if x is None or np.isscalar(x):
            #if target:
            #    logger.warn("WARNING: assign_variables promoting scalar %s to series" % target)
            x = pd.Series([x] * len(locals_dict[parcel_df_name].index),
                          index=locals_dict[parcel_df_name].index)
        if not isinstance(x, pd.Series):
            x = pd.Series(x)
        x.name = target

        return x

@inject.step()
def create_controls_table(settings, configs_dir):
    expression_file_path = os.path.join(configs_dir, settings['controls_expression_file'])
    spec = read_spec(expression_file_path)
    df_list = []
    for county in settings['counties']:
        df = get_acs_data(settings['census_key'], spec, settings['state'], county, settings['census_year'],)
        df_list.append(df)
    acs_table = pd.concat(df_list) 
    acs_table.reset_index(inplace = True)
    inject.add_table('all_acs', acs_table)
    controls_table = create_controls(spec)
    inject.add_table('combined_acs', controls_table)

    print 'done'
