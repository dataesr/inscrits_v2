from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

INITIALISATION = False

if INITIALISATION == True:

    sise_read(PATH)

    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    sise_config()
    get_all_correctifs_from_google()
    bcn_complete()
    # get_paysageODS("fr-esr-paysage_structures_identifiants", "paysage_id")
    

# STEP 2
last_data_year = last_year_into_zip(f"{PATH}output/")

etab = etab_update(last_data_year)
meef = etabli_meef(last_data_year)


# vars_sise_to_be_check(last_data_year)

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
for rentree in ALL_RENTREES:
    df = data_cleansing(last_data_year, rentree, etab, meef)

    
# si besoin de vérifier une source spécifique sans traitement
# df=get_individual_source('inge', '2016')
# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')



