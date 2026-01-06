import os
import re

import pandas as pd
import numpy as np

from census_getter.util import Util
from census_getter.census_helpers import Census


def call_census_api(county, spec, util):
        state = util.settings['state']
        census_year = util.settings['census_year'] 
        if util.settings['tract'] ==  'None':
            tract = None
        else:
            tract = util.settings['tract']

        c = Census(os.environ[util.settings['census_key']])

        hh_bg_columns = get_column_names('block_group', 'household', spec)
        hh_tract_columns = get_column_names('tract', 'household', spec)

        if len([x for x in hh_bg_columns if x in hh_tract_columns]) > 0:
            raise RuntimeError("The same acs column is being used as block group and tract. Please check expression file.")

        h_acs = c.block_group_and_tract_query(
            hh_bg_columns, hh_tract_columns, state, county,
            merge_columns=['tract', 'county', 'state'],
            block_group_size_attr=util.settings['hh_bg_size_attr'],
            tract_size_attr=util.settings['hh_tract_size_attr'],
            tract=tract,
            year=census_year)
        
        pers_bg_columns = get_column_names('block_group', 'person', spec)
        pers_tract_columns = get_column_names('tract', 'person', spec)

        if len([x for x in pers_bg_columns if x in pers_tract_columns]) > 0:
            raise RuntimeError("The same acs column is being used as block group and tract. Please check expression file.")

        p_acs = c.block_group_and_tract_query(
            pers_bg_columns, pers_tract_columns, state, county,
            merge_columns=['tract', 'county', 'state'],
            block_group_size_attr=util.settings['pers_bg_size_attr'],
            tract_size_attr=util.settings['pers_tract_size_attr'],
            tract=tract,
            year = census_year)

        all_acs = h_acs.merge(p_acs, how = 'left', on = ['state', 'county', 'tract', 'block group'])
        
        return all_acs

def create_controls(spec, util):
        locals_d = {'df' : util.get_table('all_acs')}
        
        le = []
        
        for e in zip(spec.geog, spec.target,
                     spec.expression):
            geog, target, expression = e

            values = to_series(eval(expression, globals(), locals_d), target=target)
            le.append((target, values))
       
        variables = {}
        seen = set()
        for statement in reversed(le):
            # statement is a tuple (<target_name>, <eval results in pandas.Series>)
            target_name = statement[0]
            if target_name not in seen:
                variables[statement[0]] = statement[1]
                #variables.insert(0, statement)
                seen.add(target_name)


        variables = pd.DataFrame.from_dict(variables)
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
            x = pd.Series([x] * len(locals_dict[parcel_df_name].index),
                          index=locals_dict[parcel_df_name].index)
        if not isinstance(x, pd.Series):
            x = pd.Series(x)
        x.name = target

        return x

def get_acs_data(util):
    print("Getting ACS data from Census API...")
    data_dir = util.get_data_dir()
    expression_file = util.settings['controls_expression_file']
    expression_file_path = os.path.join(util.get_settings_path(), expression_file)
    spec = read_spec(expression_file_path)
    df_list = []
    for county in util.settings['counties']:
        df = call_census_api(county, spec, util)
        df_list.append(df)
    acs_table = pd.concat(df_list) 
    acs_table.reset_index(inplace = True)
    util.save_table('all_acs', acs_table)
    controls_table = create_controls(spec,util)
    controls_table = util.create_full_block_group_id(controls_table)
    util.save_table('combined_acs', controls_table)

def run_step(context):
    util = Util(settings_path=context['configs_dir'])
    get_acs_data(util)
    return context