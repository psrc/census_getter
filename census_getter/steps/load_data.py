import pandas as pd
import yaml
from util.util import Util

util = Util()
settings = util.settings


def load_tables():
    # Creates an HDF5 file and loads tables into it
    table_list = util.get_table_list()
    with pd.HDFStore(f"{util.get_data_dir()}/pipeline.h5", mode='w') as h5store:
        for table in table_list:
            print(f"Loading table: {table['tablename']} from file: {table['filename']}")
            df = pd.read_csv(f"{util.get_data_dir()}/{table['filename']}",low_memory=False)
                        # fill nan values
            df = util.fill_nan_values(df)
            h5store.put(table['tablename'], df, format='table')

def run_step(context):
    # pypyr step to run load_data.py
    print("Loading data into HDF5 store...")
    load_tables()
    return context