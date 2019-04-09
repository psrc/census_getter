import logging
import os
import re

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import assign


from census_getter.util import setting
from synthpop.census_helpers import Census

logger = logging.getLogger(__name__)

@inject.step()
def apply_acs_shares(settings, configs_dir):