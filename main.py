from library_import import *
from config_path import PATH

INITIALISATION = False

if INITIALISATION == True:
    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    sise_read(PATH)
    get_all_correctifs_from_google()


last_data_year = last_year_into_zip(f"{PATH}output/")

global PAYSAGE_id, CORRECTIFS_dict, BCN

# STEP 2 - etabli + paysage
if 'paysage_id' in globals():
    print("paysage_id exists.")
else:
    print("paysage_id does not exist. Loading it now.")
    PAYSAGE_id = get_paysageODS("fr-esr-paysage_structures_identifiants")
    
etabli = etabli_paysage(last_data_year, PAYSAGE_id)


CORRECTIFS_dict = get_all_correctifs()
BCN = bcn_complete()
modal_sise = vars_sise_to_be_check(last_data_year, BCN, CORRECTIFS_dict)

# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')



