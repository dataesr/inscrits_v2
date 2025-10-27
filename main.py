from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

last_data_year = last_year_into_zip(f"{PATH}output/", 'sise_parquet')
ALL_RENTREES = list(range(2004, int(last_data_year)+1))

INITIALISATION = False
CLEANING = False
OUTPUT = True

if INITIALISATION == True:
    # read sise file, select vars, create of excel files to check data, save concatenate data into parquet file per year
    sise_read(PATH)
    last_data_year = last_year_into_zip(f"{PATH}output/", 'sise_parquet')

    get_all_correctifs_from_google()
    update_ref_data('correctifs.json')
    bcn_complete()
    update_ref_data('bcn.pkl')
    get_paysageODS()
    update_ref_data('paysage.json')
    
if CLEANING == True:

    ### si besoin d'ajouter ou corriger RATTACH uai
    # rattach_single_add('0292136P', '0292078B', range(2004, 2025))


    etab = etab_update(last_data_year)
    etab = enrich_paysage(etab)
    meef = etabli_meef(last_data_year)


    # vars_sise_to_be_check(last_data_year, 'origin')


    zipin_path = os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
    zipout_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")
    # cols_selected = yaml.safe_load(open("utils/variables_selection.yaml", "r") )
    # df_all = pd.DataFrame()
    # df_items = pd.DataFrame()
    # df_com = pd.DataFrame()
    # df_bac = pd.DataFrame()


    for rentree in ALL_RENTREES:
        # from utils.vars import  calcs_select, cols_select 
        df = get_individual_source(zipin_path, 'sise', rentree)
        df = data_result(df, etab, meef)
        data_save_by_year(rentree, df, 'sise', zipout_path)
        # it_list = check_items_list(df)
        # df_items = pd.concat([df_items, it_list])
        # df_bac = pd.concat([df_bac, df[['rentree', 'anbac', 'bac', 'bac_rgrp_orig', 'bac_rgrp', 'effectif']]])
        # df_com = pd.concat([df_com, df[['rentree', 'etabli', 'id_paysage', 'lib_paysage', 'cometa', 'ui', 'comui']].drop_duplicates()])
        # df_all = (pd.concat([df_all, 
        #                     df[cols_selected["all_select"]]
        #                     .groupby(list(set(cols_selected["variables_all"]).difference(set(cols_selected["variables_all_num"]))), dropna=False)[cols_selected['variables_all_num']]
        #                     .sum()
        #                     .reset_index()
        # ]))
        


if OUTPUT == True:
    
    zipin_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")
    od_complete = pd.DataFrame()

    for rentree in ALL_RENTREES:
        # sas opendata19 -> 259 lignes
        df = get_individual_source(zipin_path, 'sise', rentree)
        od = opendata_first(df)
        od_complete = pd.concat([od_complete, od], ignore_index=True)
    
    od_complete = od_first_enrich(od_complete)
    od_tableau(od_complete)
    od, od2p, testtypo = od_create_files(od_complete)




# df_all.to_pickle(f"{PATH}output/sise_etab_{last_data_year}.pkl")
# etab_checking(int(last_data_year), df_all)
# df_all.to_parquet(f"{PATH}output/sise_etab_{last_data_year}.parquet", compression='gzip')
# df_com.to_pickle(f"{PATH}output/sise_com_{last_data_year}.pkl")
# df_items.to_pickle(f"{PATH}output/items_cleaned_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
# checking_final_data(last_data_year, df)





# vars_sise_to_be_check(last_data_year, 'cleaned', df_all)
# commune_checking(last_data_year, df_com)
# bac_to_check(last_data_year, df_bac)

# si besoin de vérifier une source spécifique sans traitement
zip_path=os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")
# zip_path=os.path.join(PATH, f"input/parquet_origine_{last_data_year}.zip")
df=get_individual_source(zip_path, 'sise', '2022')
# df = data_cleansing(df, etab, meef)
# from test_py import work_csv
# work_csv(x, 'vars_hs_nomen')

# df.loc[df.diplom=='-1', ['rentree','source', 'etabli', 'id_paysage', 'lib_paysage', 'compos', 'typ_dipl', 'cursus_lmd', 'sectdis']].drop_duplicates()
# work_csv(x, 'diplom_empty')
