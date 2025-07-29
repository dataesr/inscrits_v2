
import warnings, pandas as pd
warnings.filterwarnings("ignore", 'This pattern has match groups')
pd.options.mode.copy_on_write = True

from modules.sise_read import *
from modules.sise_content import *
from nomenclatures.bcn import *
from nomenclatures.google_sheet import *
from nomenclatures.paysage import *
from utils.config_sise import *
from modules.variables_check import *
from utils.functions_shared import *
