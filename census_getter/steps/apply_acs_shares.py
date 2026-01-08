import os
import pandas as pd

from census_getter.util import Util


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

def apply_acs_shares(util):
    # Load expression spec
    data_dir = util.get_data_dir()
    expression_file = util.settings['apply_acs_shares_expression_file']
    expression_file_path = os.path.join(util.get_settings_path(), expression_file)
    spec = read_spec(expression_file_path)

    # get tables and merge into one dataframe
    input_table_list = [util.settings['input_table_list'][i]['tablename'] for i in range(len(util.settings['input_table_list']))]
    df = pd.DataFrame()
    for table_name in input_table_list:
        table = util.get_table(table_name)
        # merge into one dataframe
        df = df.merge(table, on='block_group_id', how='outer') if not df.empty else table
    combined_acs = util.get_table('combined_acs')
    df = df.merge(combined_acs, on='block_group_id', how='left')
    df = df.set_index('block_group_id')
    df = df.astype(float)
    util.save_table('all_input_data', df)

    # evaluate expressions to create new columns
    df = eval_expressions(spec, df)
    df = util.fill_nan_values(df)

    # drop extra columns (totals) and round final values
    tot_cols = tot_cols_to_list(df)
    df = df.drop(columns=tot_cols).round(0)

    # save final dataframe
    util.save_table('ofm_controls', df)

def run_step(context):
    print("Applying OFM shares to ACS data...")
    util = Util(settings_path=context['configs_dir'])
    apply_acs_shares(util)
    return context