from pathlib import Path
import subprocess
from census_getter.util import Util

def run_step(context):
    print("Running populationsim...")
    util = Util(settings_path=context['configs_dir'])
    # get root path relative to the given configs_dir (one level up)
    root_path = Path(context['configs_dir']).parent

    configs_popsim = util.settings['configs_popsim']
    popsim_configs_dir = root_path / configs_popsim
    data_dir = root_path / 'data'
    output_dir = root_path / 'output'

    returncode = subprocess.call([
        ".venv/Scripts/python.exe", "-m", "populationsim", 
        '--config', str(popsim_configs_dir),
        '--data', str(data_dir),
        '--output', str(output_dir)
        ])

    return context
    