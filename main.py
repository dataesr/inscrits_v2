from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

INITIALISATION = False
if INITIALISATION == True:
    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    sise_read(PATH)

    sise_config()
    get_all_correctifs_from_google()
    update_ref_data('correctifs.json')
    bcn_complete()
    update_ref_data('bcn.pkl')
    get_paysageODS()
    update_ref_data('paysage.json')
    

# STEP 2
last_data_year = last_year_into_zip(f"{PATH}output/", 'sise_parquet')

### si besoin d'ajouter ou corriger RATTACH uai
# rattach_single_add('0292136P', '0292078B', range(2004, 2025))


etab = etab_update(last_data_year)
etab = enrich_paysage(etab)
meef = etabli_meef(last_data_year)


vars_sise_to_be_check(last_data_year, 'origin')


zipin_path = os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
zipout_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")

df_all = pd.DataFrame()
df_items = pd.DataFrame()

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
for rentree in ALL_RENTREES:
    df = get_individual_source(zipin_path, 'sise', rentree)
    df = data_cleansing(df, etab, meef)
    it_list = check_items_list(df)
    df_items = pd.concat([df_items, it_list])
    df_all = pd.concat([df_all, df[['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'rattach', 'compos',  'uai_fresq', 'inf', 'inspr', 'cursus_lmd', 'typ_dipl', 'diplom', 'effectif']]])
    data_save_by_year(rentree, df, 'sise', zipout_path)

df_all.to_pickle(f"{PATH}work/sise_etab_{last_data_year}.pkl")
df_items.to_pickle(f"{PATH}output/items_cleaned_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
etab_checking(int(last_data_year), df_all)
vars_sise_to_be_check(last_data_year, 'cleaned')


# si besoin de vérifier une source spécifique sans traitement
zip_path=os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
# zip_path=os.path.join(PATH, f"input/parquet_origine.zip")
df=get_individual_source(zip_path, 'sise', '2004')
# df = data_cleansing(df, etab, meef)
# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')

df.loc[df.diplom=='-1', ['rentree','source', 'etabli', 'id_paysage', 'lib_paysage', 'compos', 'typ_dipl', 'cursus_lmd', 'sectdis']].drop_duplicates()
# work_csv(x, 'diplom_empty')