import os
import logging
os.chdir(r'E:\census_getter\census_getter\example_psrc')
working_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(working_dir, os.pardir))
os.sys.path.append(os.path.join(parent_dir))
os.sys.path.append(os.path.join(parent_dir,'census_getter'))

from activitysim.core import inject
from census_getter import steps

from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject

from activitysim.core.config import handle_standard_args
from activitysim.core.tracing import print_elapsed_time

from census_getter.util import setting

handle_standard_args()

tracing.config_logger()

t0 = print_elapsed_time()

logger = logging.getLogger('census_getter')

# get the run list (name was possibly specified on the command line with the -m option)
run_list_name = inject.get_injectable('run_list_name', 'run_list')

# run list from settings file is dict with list of 'steps' and optional 'resume_after'
run_list = setting(run_list_name)
assert 'steps' in run_list, "Did not find steps in run_list"

# list of steps and possible resume_after in run_list
steps = run_list.get('steps')
resume_after = run_list.get('resume_after', None)

if resume_after:
    print ("resume_after", resume_after)

pipeline.run(models=steps, resume_after=resume_after)


# tables will no longer be available after pipeline is closed
pipeline.close_pipeline()

t0 = ("all models", t0)
