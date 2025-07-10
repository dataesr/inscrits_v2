def sise_read(path):
    import os
    import pandas as pd
    from step1_data_init.sise_content import zip_content, vars_compare, src_load, data_save
    from utils.functions_shared import get_sources
    

    ### liste les noms des datasets présents dans le zip parquet, extraction de la dernière années des données dispo
    dataset_list, last_data_year = zip_content()
    print(f"dernière rentrée de sise dispo: {last_data_year}")


    ### Création du fichier vide excel DATA_REVIEWW_(last_year) pour contrôler les variables
    # Construire le chemin complet du fichier Excel en utilisant l'année fournie
    excel_path = os.path.join(path, f"data_review_{last_data_year}.xlsx")

    # Vérifier si le fichier Excel existe déjà
    if not os.path.exists(excel_path):
        # Si le fichier n'existe pas, le créer en mode écriture
        with pd.ExcelWriter(excel_path, mode='w', engine='openpyxl') as writer:
            # Écrire le DataFrame dataset_list dans une feuille nommée 'l1_datasets'
            dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)
    else:
        # Si le fichier existe déjà, l'ouvrir en mode ajout
        with pd.ExcelWriter(excel_path, mode="a", if_sheet_exists="replace", engine='openpyxl') as writer:
            # Remplacer la feuille 'l1_datasets' si elle existe déjà, sinon la créer
            dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)



    # Chargement des tables et création d'une base complète df_all sauvé au format parquet
    ### Ajout des infos sur l'état des sources dans 
    # etat des variables par année source
    vars_review = pd.DataFrame(columns=['variable', 'ex', 'source', 'rentree'])
    df_items = pd.DataFrame()

    ALL_RENTREES = list(range(2004, int(last_data_year)+1))
    for rentree in ALL_RENTREES:
        df_all = pd.DataFrame()
        sources = get_sources(rentree)
        for source in sources:

            filename = f'{source}{str(rentree)[2:4]}'
            print(filename)

            # nettoyage des variables suppression des vars en doublon commençant par 'S'
            df = vars_compare(filename, source, rentree)
            vars_review = pd.concat([vars_review, df], ignore_index=True)

            # chargement des tables en conservant que les variables de la liste utils/vars_list
            df = src_load(excel_path, filename, source, rentree)
            df_all = pd.concat([df_all, df], ignore_index=True)

        # sauvegarde dans output d'un sise complet par année au format parquet
        data_save(rentree, df_all, last_data_year)
        for i in df_all.columns.difference(['rentree', 'source']):
            tmp = df_all.groupby(['rentree', 'source'])[i].value_counts(dropna=False).reset_index().rename(columns={i:'item'}).assign(variable=i)
            df_items = pd.concat([df_items, tmp])
            del tmp
        tmp = (df_all.groupby(['rentree', 'source', 'etabli', 'compos'])
               .agg(sum_var=('effectif', 'sum'), count_rows=('effectif', 'size'))
               .reset_index())
    print("- export completed sise_parquet")

    with pd.ExcelWriter(excel_path, mode='a', if_sheet_exists="replace") as writer:  
        vars_review.to_excel(writer, sheet_name='l2_vars_source_year', index=False)

    # creation d'un fichier pkl avec toutes les modalités par var pour contrôle
    tmp.to_pickle(f"{path}output/frequency_etabli_source_year{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
    df_items.mask(df_items=='', inplace=True)
    df_items.to_pickle(f"{path}output/items_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
