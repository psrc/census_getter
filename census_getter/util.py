import pandas as pd
import yaml



class Util:
    def __init__(self, settings_path='configs'):
        """
        Initialize Util with settings loaded from a YAML file.
        """
        self.settings_path = settings_path
        
        with open(f"{self.settings_path}/settings.yaml", 'r') as file:
            self.settings = yaml.safe_load(file)

    def get_settings_path(self):
        # Returns the path to the settings directory
        return self.settings_path

    def get_data_dir(self):
        # Returns the data directory path from settings.yaml
        return self.settings.get('data_dir', 'data')
    
    def get_output_dir(self):
        # Returns the output directory path from settings.yaml
        return self.settings.get('output_dir', 'output')

    def get_table_list(self):
        # Returns a list of table names from settings.yaml
        return self.settings.get('input_table_list', [])
    
    def get_output_table_list(self):
        # Returns a list of output table names from settings.yaml
        return self.settings.get('output_table_list', [])

    def get_table(self, table_name):
        with pd.HDFStore(f"{self.get_data_dir()}/pipeline.h5", mode='r') as h5store:
            return h5store.get(table_name)

    def save_table(self, table_name, df):
        with pd.HDFStore(f"{self.get_data_dir()}/pipeline.h5", mode='a') as h5store:
            h5store.put(table_name, df, format='table')

    def create_full_block_group_id(self, my_table):
        df = self.get_table(my_table)
        cols = ['state', 'county', 'tract', 'block group']
        # concatenate to create full block group id using zfill to ensure proper lengths
        df['block_group_id'] = df['state'].astype(str).str.zfill(2) + \
                               df['county'].astype(str).str.zfill(3) + \
                               df['tract'].astype(str).str.zfill(6) + \
                               df['block group'].astype(str)
        df = df.drop(columns=cols)
        self.save_table(my_table, df)

    def fill_nan_values(self, df):
        if 'nan_fill' in self.settings:
            df = df.fillna(self.settings['nan_fill'])
        return df