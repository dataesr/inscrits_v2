
import warnings, pandas as pd
warnings.filterwarnings("ignore", 'This pattern has match groups')
pd.options.mode.copy_on_write = True

from step1_data_init.sise_read import *
from step1_data_init.sise_content import *
from nomenclatures.bcn import *
from nomenclatures.google_sheet import *
from utils.config import *
from step2_first_control.variables_check import *
from utils.functions_shared import *
from api_process.paysage import *