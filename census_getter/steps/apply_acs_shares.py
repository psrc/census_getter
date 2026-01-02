import os
import pandas as pd
from iteround import saferound

from census_getter.util import Util

util = Util()
settings = util.settings

def read_spec(fname):
    cfg = pd.read_csv(fname, comment='#')
    
    # backfill description
    if 'Description' not in cfg.columns:
        cfg.description = ''

    cfg.target = cfg.target.str.strip()
    cfg.target = cfg.target.replace('-', '_', regex=True)
    cfg.expression = cfg.expression.str.strip()

    return cfg

def eval_expressions(spec, df):
    # for each row in spec, create new columns in df according to the expression
    spec = spec.sort_values(by='target', ascending=True)
    for index, row in spec.iterrows():
        new_col_name = row['target']
        # Remove 'df.' prefix from the expression
        expression = row['expression'].replace('df.', '')
        df.eval(f"{new_col_name} = {expression}", inplace=True, engine='python')
    return df

def tot_cols_to_list(df):
    tot_cols = []
    for col in df.columns:
        if col[0] == '_':
            tot_cols.append(col)
    return tot_cols

def round_grouped_columns(df, spec, tot_cols):
    for tot_col in tot_cols:
        group_expr = spec.loc[spec['target'] == tot_col,'expression'].values[0]
        group_cols = [col.strip().replace('df.','') for col in group_expr.split('+')]
        for index,row in df[group_cols].iterrows():
            round_list = row[group_cols].values.tolist()
            round_list = saferound(round_list,places=0)
            df.loc[index,group_cols] = round_list
            df[group_cols] = df[group_cols].astype(int)
    return df

def apply_acs_shares():
    # Load expression spec
    data_dir = util.get_data_dir()
    expression_file_path = settings['apply_acs_shares_expression_file']
    spec = read_spec(expression_file_path)

    # get tables and merge into one dataframe
    input_table_list = [settings['input_table_list'][i]['tablename'] for i in range(len(settings['input_table_list']))]
    df = pd.DataFrame()
    for table_name in input_table_list:
        util.create_full_block_group_id(table_name)
        table = util.get_table(table_name)
        # merge into one dataframe
        df = df.merge(table, on='block_group_id', how='outer') if not df.empty else table
    combined_acs = util.get_table('combined_acs')
    df = df.merge(combined_acs, on='block_group_id', how='left')
    df = df.set_index('block_group_id')
    df = df.astype(float)

    # evaluate expressions to create new columns
    df = eval_expressions(spec, df)
    df = util.fill_nan_values(df)

    # round grouped columns to ensure sums match totals
    tot_cols = tot_cols_to_list(df)
    df = round_grouped_columns(df, spec, tot_cols)

    # save final dataframe
    util.save_table('all_input_data', df)

def run_step(context):
    print("Applying OFM shares to ACS data...")
    apply_acs_shares()
    return context