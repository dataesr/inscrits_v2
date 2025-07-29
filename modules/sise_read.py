def sise_read(path):
    import pandas as pd
    from modules.sise_content import zip_content, src_load, data_save, vars_init
    from utils.functions_shared import get_sources
    

    ### liste les noms des datasets présents dans le zip parquet, extraction de la dernière années des données dispo
    dataset_list, last_data_year = zip_content()
    print(f"dernière rentrée de sise dispo: {last_data_year}")


    # Chargement des tables et création d'une base complète df_all sauvé au format parquet par année
    ### Ajout des infos sur l'état des sources dans 
    # etat des variables par année source
    df_items = pd.DataFrame()
    uai_correctif = pd.DataFrame()

    ALL_RENTREES = list(range(2004, int(last_data_year)+1))
    for rentree in ALL_RENTREES:
        df_all = pd.DataFrame()
        sources = get_sources(rentree)
        for source in sources:

            filename = f'{source}{str(rentree)[2:4]}'
            print(filename)

            # chargement des tables en conservant que les variables de la liste utils/vars_list
            df = src_load(filename, source, rentree) 
            df_all = pd.concat([df_all, df], ignore_index=True)
            
        df_all = vars_init(df_all)

        # sauvegarde dans output d'un sise complet par année au format parquet
        data_save(rentree, df_all, last_data_year)
        for i in df_all.columns.difference(['rentree', 'source']):
            tmp = df_all.groupby(['rentree', 'source'])[i].value_counts(dropna=False).reset_index().rename(columns={i:'item'}).assign(variable=i)
            df_items = pd.concat([df_items, tmp])
            del tmp
        
        uai_correctif = pd.concat([uai_correctif, (df_all.groupby(['rentree', 'source', 'etabli', 'compos'])
               .agg(effectif_tot=('effectif', 'sum'), count_rows=('effectif', 'size'))
               .reset_index())])
    print("- export completed sise_parquet")

    # creation d'un fichier pkl avec toutes les modalités par var pour contrôle
    uai_correctif.to_pickle(f"{path}output/frequency_uai_source_year{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
    df_items.mask(df_items=='', inplace=True)
    df_items.to_pickle(f"{path}output/items_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
