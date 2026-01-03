import os
from census_getter.util import Util


def create_output_dir(util):
    output_dir = util.get_output_dir()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def write_tables(util):
    # Write prepared tables to CSV files
    table_list = util.get_output_table_list()
    for table in table_list:
        df = util.get_table(table['tablename'])
        output_path = f"{util.get_output_dir()}/{table['filename']}"
        print(f"Writing table: {table['tablename']} to file: {output_path}")
        df.to_csv(output_path)

def run_step(context):
    print("Writing output tables...")
    util = Util(settings_path=context['configs_dir'])
    create_output_dir(util)
    write_tables(util)
    return context