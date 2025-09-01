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
    


# # chargement des nomenclatures
# CORRECTIFS_dict = reference_data_loader('google_sheet')
# BCN = reference_data_loader('bcn')
# PAYSAGE_id = reference_data_loader('paysage_id') 

# STEP 2
last_data_year = last_year_into_zip(f"{PATH}output/")

rattach = rattach_init(last_data_year)
meef = etabli_meef(last_data_year)


vars_sise_to_be_check(last_data_year)


df=data_cleansing(2024, 2024, meef)
# si besoin de vérifier une source spécifique sans traitement
# df=get_individual_source('mana', '2013')
# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')



