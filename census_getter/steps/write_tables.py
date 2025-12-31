from util.util import Util

util = Util()
settings = util.settings


def write_tables():
    # Write prepared tables to CSV files
    table_list = util.get_output_table_list()
    for table in table_list:
        df = util.get_table(table['tablename'])
        output_path = f"{util.get_output_dir()}/{table['filename']}"
        print(f"Writing table: {table['tablename']} to file: {output_path}")
        df.to_csv(output_path)

def run_step(context):
    write_tables()
    return context