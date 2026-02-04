from pathlib import Path
from pypyr import pipelinerunner
import subprocess


# no command line args in this example

#----------------------------------------------------------
# Run census_getter to download and prepare PUMS and ACS data
#----------------------------------------------------------
# census_getter_configs_dir = Path(__file__).parent / "configs_census_getter"
# pipelinerunner.run(f'{census_getter_configs_dir}/settings', dict_in={'configs_dir': census_getter_configs_dir})


#----------------------------------------------------------
# Run populationsim
#----------------------------------------------------------
popsim_configs_dir = Path(__file__).parent / "configs_popsim"
data_dir = Path(__file__).parent / "data"
output_dir = Path(__file__).parent / "output"

returncode = subprocess.call([
    ".venv/Scripts/python.exe", "-m", "populationsim", 
    '--config', str(popsim_configs_dir),
    '--data', str(data_dir),
    '--output', str(output_dir)
    ])