def sise_read(path):
    import pandas as pd, numpy as np
    from modules.sise_content import zip_content, src_load, data_save, vars_init, rattach_init
    from utils.functions_shared import get_sources
    

    ### liste les noms des datasets présents dans le zip parquet, extraction de la dernière années des données dispo
    dataset_list, last_data_year = zip_content()
    print(f"dernière rentrée de sise dispo: {last_data_year}")


    # Chargement des tables et création d'une base complète df_all sauvé au format parquet par année
    ### Ajout des infos sur l'état des sources dans 
    # etat des variables par année source
    # df_items = pd.DataFrame()
    uai_correctif = pd.DataFrame()
    meef = pd.DataFrame()

    ALL_RENTREES = list(range(2004, int(last_data_year)+1))
    for rentree in ALL_RENTREES:
        df_all = pd.DataFrame()
        sources = get_sources(rentree)
        for source in sources:

            filename = f'{source}{str(rentree)[2:4]}'
            print(filename)

            # chargement des tables en conservant que les variables de la liste utils/vars_list
            df = src_load(last_data_year, filename, source, rentree) 
            df_all = pd.concat([df_all, df], ignore_index=True)
            
        df_all = vars_init(df_all)
        rattach = rattach_init(rentree)
        df_all = df_all.merge(rattach, how='left', on=['rentree', 'compos'])

        # sauvegarde dans output d'un sise complet par année au format parquet
        data_save(rentree, df_all, last_data_year)
        # res = check_items_list(df_all)
        # df_items = pd.concat([df_items, res])
        
        uai_correctif = pd.concat([uai_correctif, df_all[['rentree', 'source', 'etabli', 'compos', 'rattach', 'cometa', 'comins', 'effectif']]])
        
        if 'etabli_diffusion' in df_all.columns or 'flag_meef' in df_all.columns:
            meef = pd.concat([meef, df_all.loc[~df_all.etabli_diffusion.isnull(), ['rentree', 'source', 'etabli', 'etabli_diffusion', 'flag_meef', 'typ_dipl', 'diplom', 'effectif']]])
    print("- export completed sise_parquet")

    # creation d'un fichier pkl avec toutes les modalités par var pour contrôle
    meef.loc[meef.flag_meef=='1', ['typ_dipl', 'diplom']] = np.nan
    (meef.groupby(['rentree', 'source', 'etabli', 'etabli_diffusion', 'flag_meef', 'typ_dipl', 'diplom'], dropna=False)
        .agg(effectif_tot=('effectif', 'sum'), count_rows=('effectif', 'size'))
        .reset_index()
        .to_pickle(f"{path}output/meef_frequency_source_year{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1}))
    
    (uai_correctif.groupby(['rentree', 'source', 'etabli', 'compos', 'rattach', 'cometa', 'comins'], dropna=False)
               .agg(effectif_tot=('effectif', 'sum'), count_rows=('effectif', 'size'))
               .reset_index()
               .to_pickle(f"{path}output/uai_frequency_source_year{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1}))
    
    # df_items.mask(df_items=='', inplace=True)
    # df_items.to_pickle(f"{path}output/items_origin_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
