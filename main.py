from library_import import *
from config_path import PATH
warnings.simplefilter("ignore")

last_data_year = last_year_into_zip(f"{PATH}output/", 'sise_parquet')
ALL_RENTREES = list(range(2004, int(last_data_year)+1))

INITIALISATION = False
CLEANING = True
DEBUG = False
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

    ### si besoin d'ajouter ou corriger RATTACH uai à la marge
    # rattach_single_add('0292136P', '0292078B', range(2004, 2025))


    etab = etab_update(last_data_year)
    meef = etabli_meef(last_data_year)

    if DEBUG == True:
        vars_sise_to_be_check(last_data_year, 'origin')

        cols_selected = yaml.safe_load(open("utils/variables_selection.yaml", "r"))
        df_all = pd.DataFrame()
        df_items = pd.DataFrame()
        df_com = pd.DataFrame()
        df_bac = pd.DataFrame()


    # Read sise PARQUET and write sise CLEANED pickle
    zipin_path = os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
    zipout_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")

    for rentree in ALL_RENTREES:
        df = get_individual_source(zipin_path, 'sise', rentree)
        df = data_result(df, etab, meef)
        data_save_by_year(rentree, df, 'sise', zipout_path)

        if DEBUG == True:
            it_list = check_items_list(df)
            df_items = pd.concat([df_items, it_list])
            df_bac = pd.concat([df_bac, df[['rentree', 'anbac', 'bac', 'bac_rgrp_orig', 'bac_rgrp', 'effectif']]])
            df_com = pd.concat([df_com, df[['rentree', 'etabli', 'id_paysage', 'lib_paysage', 'cometa', 'ui', 'comui']].drop_duplicates()])
            df_all = (pd.concat([df_all, 
                                df.groupby(list(set(cols_selected["variable_all"]).difference(set(cols_selected["variable_all_num"]))), dropna=False)[cols_selected['variable_all_num']].sum().reset_index()
            ]))
            df_all.to_pickle(f"{PATH}output/sise_etab_{last_data_year}.pkl")
            etab_checking(int(last_data_year), df_all)
            df_com.to_pickle(f"{PATH}output/sise_com_{last_data_year}.pkl")
            df_items.to_pickle(f"{PATH}output/items_cleaned_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
            checking_final_data(last_data_year, df)
            vars_sise_to_be_check(last_data_year, 'cleaned', df_all)
            commune_checking(last_data_year, df_com)
            bac_to_check(last_data_year, df_bac)


if OUTPUT == True:
    zipin_path = os.path.join(PATH, f"output/sise_cleaned_{last_data_year}.zip")
    od_complete = pd.DataFrame()
    # verif = pd.DataFrame()

    for rentree in ALL_RENTREES:
        df = get_individual_source(zipin_path, 'sise', rentree)
        # verif = pd.concat([verif, df[['lmddontbis', 'typ_dipl', 'efft']]])
        od_first = opendata_first(df)
        od_complete = pd.concat([od_complete, od_first], ignore_index=True)
    
    od_complete = od_first_enrich(od_complete)
    od_tableau(od_complete)
    od, od2p = od_create_files(od_complete)
    od_synthese_by_etab(od)
    od_synthese_by_inspe(od)
    od_synthese_for_paysage(od2p)
    od_synthese_by_diplom(od_complete)
