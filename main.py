from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

INITIALISATION = False

if INITIALISATION == True:
    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    # sise_config()
    sise_read(PATH)
    get_all_correctifs_from_google()
    bcn_complete()
    get_paysageODS("fr-esr-paysage_structures_identifiants")

last_data_year = last_year_into_zip(f"{PATH}output/")

vars_sise_to_be_check(last_data_year)

# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')



