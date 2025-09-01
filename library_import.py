
import warnings, pandas as pd
warnings.filterwarnings("ignore", 'This pattern has match groups')
pd.options.mode.copy_on_write = True

global CORRECTIFS_dict, BCN, PAYSAGE_id

from modules.sise_read import *
from modules.sise_content import *
from reference_data.bcn import *
from reference_data.google_sheet import *
from reference_data.paysage import *
from reference_data.ref_data_utils import *
from utils.config_sise import *
from modules.variables_check import *
from utils.functions_shared import *
from modules.etab_fixes import *
from modules.cleansing import *

