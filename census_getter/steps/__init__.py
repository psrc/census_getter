from activitysim.core import inject

from . import pums_download
from . import prepare_pums
from . import get_acs_data
from . import write_tables
from . import input_pre_processor
from . import apply_acs_shares

from activitysim.core.steps.output import write_data_dictionary
#from activitysim.core.steps.output import write_tables


@inject.injectable(cache=True)
def preload_injectables():
    inject.add_step('write_data_dictionary', write_data_dictionary)
    #inject.add_step('write_tables', write_tables)
    return True