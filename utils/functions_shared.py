import pandas as pd

def get_sources(annee):
    # selection des sources existantes en fonction des années
    assert(annee >= 2004)
    sources = ['inscri', 'inge', 'priv']
    #sources = ['inge', 'priv']
    if 2004 <= annee <= 2007:
        sources.append('iufm')
    if annee > 2004:
        sources.append('ens')
    if annee > 2005:
        sources.append('mana')
    if annee > 2015:
        sources.append('enq26bis')
    if annee > 2016:
        sources.append('culture')
    return sources


def last_file_into_folder(path, extension, prefix):
    import os
    files_list = [f for f in os.listdir(path) if f.endswith(f'.{extension}') and f.startswith(prefix)]

    # Trouver le fichier le plus récent
    latest_file = max(files_list, key=lambda f: os.path.getmtime(os.path.join(path, f)))

    # Chemin complet vers le fichier le plus récent
    print(f"Le fichier {extension} le plus récent est : {os.path.join(path, latest_file)}")
    return os.path.join(path, latest_file)


def last_year_into_zip(path, prefix):
    # Importer le chemin de configuration depuis un module externe
    import zipfile
    zip_file_path=last_file_into_folder(path, 'zip', prefix)
    # Ouvrir le fichier ZIP en mode lecture
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        #keep the 2 last charecters of sisename and add '20'
        year = max({int('20' + item) for item in set(
            filter(None, [i.split('.')[0][-2:] for i in z.namelist()])
        )})
    print(f"the zip last year is {year}")
    return year


def get_individual_source(zip_full_path, source, rentree):
    import pandas as pd, zipfile

    filename = f'{source}{str(rentree)[2:4]}'
    print(filename)

    with zipfile.ZipFile(zip_full_path, 'r') as z:
        df = pd.read_parquet(z.open(f'{filename}.parquet'), engine='pyarrow')
    
    print(f"- {rentree} -> size: {len(df)}")

    return df


def work_csv(df, file_csv_name):
    from config_path import PATH
    import os
    PATH_WORK=f"{PATH}work/"

    if not os.path.exists(PATH_WORK):
    # Créer le dossier
        os.makedirs(PATH_WORK)
        print(f"Le dossier a été créé à l'emplacement : {PATH_WORK}")
    else:
        print(f"Le dossier existe déjà à l'emplacement : {PATH_WORK}")

    name = file_csv_name
    return df.to_csv(f"{PATH_WORK}{name}.csv", sep=';', na_rep='', encoding='utf-8', index=False)


def data_save_by_year(rentree, df, filename, zip_path):
    from config_path import PATH
    import os, zipfile

    if not os.path.exists(f'{PATH}output'):
        print("folder OUTPUT creates into DATA_PATH")
    # Create a new directory because it does not exist
        os.mkdir(f'{PATH}output')
        
    parquet_name = f'{filename}{str(rentree)[2:4]}.parquet'
    df.to_parquet(parquet_name, compression='gzip')

    print(f"Creating the parquet-files by year {parquet_name} into zip in OUTPUT")
    
    with zipfile.ZipFile(zip_path, 'a') as z:
        z.write(parquet_name)
        
    # Delete the parquet file after adding it to the ZIP
    try:
        os.remove(parquet_name)
        print(f"Deleted: {parquet_name}")
    except Exception as e:
        print(f"Error deleting {parquet_name}: {e}")


def check_items_list(df):
    import pandas as pd
    df_items = pd.DataFrame()
    for i in df.columns.difference(['rentree', 'source']):
        tmp = df.groupby(['rentree', 'source'])[i].value_counts(dropna=False).reset_index().rename(columns={i:'item'}).assign(variable=i)
        df_items = pd.concat([df_items, tmp])
        df_items.mask(df_items=='', inplace=True)
    return df_items

def replace_by_nan(serie: pd.Series) -> pd.Series:
    import numpy as np
    """Remplace None et "" par np.nan dans une série pandas."""
    return serie.replace([None, ""], np.nan)