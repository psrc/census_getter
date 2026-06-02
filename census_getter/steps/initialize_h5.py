import os
from census_getter.util import Util


def run_step(context):
    # pypyr step that deletes data/pipeline.h5 if it exists
    util = Util(settings_path=context['configs_dir'])
    pipeline_path = f"{util.get_data_dir()}/pipeline.h5"
    if os.path.exists(pipeline_path):
        print(f"Removing existing pipeline file: {pipeline_path}")
        os.remove(pipeline_path)
    else:
        print(f"No existing pipeline file found at: {pipeline_path}")
    return context
