import pandas as pd
from census_getter.util import Util


def load_tables(util):
    # Creates an HDF5 file and loads tables into it
    table_list = util.get_table_list()
    for table in table_list:
        print(f"Loading table: {table['tablename']} from file: {table['filename']}")
        df = pd.read_csv(f"{util.get_data_dir()}/{table['filename']}",low_memory=False)
        
        # fill nan values
        df = util.fill_nan_values(df)
        
        # check if block_group_id exists, if not create it
        if not util.block_group_id_exists(df):
            df = util.create_full_block_group_id(df)
        else:
            df = util.convert_col_to_int64(df, 'block_group_id')
        
        # save table to HDF5 store
        with pd.HDFStore(f"{util.get_data_dir()}/pipeline.h5", mode='a') as h5store:
            h5store.put(table['tablename'], df, format='table')

def run_step(context):
    # pypyr step to run load_data.py
    print("Loading data into HDF5 store...")
    util = Util(settings_path=context['configs_dir'])
    load_tables(util)
    return context