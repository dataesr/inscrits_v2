from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

INITIALISATION = False
if INITIALISATION == True:
    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    sise_read(PATH)

    sise_config()
    get_all_correctifs_from_google()
    bcn_complete()
    get_paysageODS()
    from reference_data.ref_data_utils import *



# STEP 2
last_data_year = last_year_into_zip(f"{PATH}output/", 'sise_parquet')

### si besoin d'ajouter ou corriger RATTACH uai
# rattach_single_add('0292136P', '0292078B', range(2004, 2025))


etab = etab_update(last_data_year)
etab = enrich_paysage(etab)
meef = etabli_meef(last_data_year)


# vars_sise_to_be_check(last_data_year)

zipin_path = os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
zipout_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")

df_all = pd.DataFrame()
df_saclay = pd.DataFrame()

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
for rentree in ALL_RENTREES:
# for rentree in [2024]:
    df = get_individual_source(zipin_path, 'sise', rentree)
    df = data_cleansing(df, etab, meef)
    df_all = pd.concat([df_all, df[['rentree', 'source', 'etabli', 'compos', 'rattach', 'id_paysage', 'lib_paysage', 'inspr', 'effectif']]])
    df_saclay = pd.concat([df_saclay, df.loc[df.etabli=='0912408Y', ['rentree', 'source', 'compos', 'typ_dipl', 'diplom', 'inspr', 'effectif']]])
    data_save_by_year(rentree, df, 'sise', zipout_path)


df_all = (df_all.groupby(list(set(df.columns).difference(set(['effectif']))), dropna=False)
          .agg({'effectif': 'sum'})
          .reset_index(name='effectif')
          )
df_all.to_pickle(f"{PATH}output/effectif_by_etab_rentree.pkl", compression='gzip')

# si besoin de vérifier une source spécifique sans traitement
zip_path=os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
df=get_individual_source(zip_path,'sise', '2016')
# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')



