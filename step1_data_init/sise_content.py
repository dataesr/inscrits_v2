
import pandas as pd, zipfile, os
import pyarrow as pa
from pyarrow.parquet import ParquetFile


def zip_content():
    # Importer le chemin de configuration depuis un module externe
    from config_path import PATH

    # Ouvrir le fichier ZIP en mode lecture
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        # Créer une série pandas à partir des noms des fichiers dans le ZIP
        # Filtrer les éléments non vides, diviser les noms de fichiers et extraire la partie pertinente
        z_content = pd.Series(
            filter(None, [i.split('.')[0].split('/')[1] for i in z.namelist()]),
            name='dataset_name'
        )

        # Extraire la dernière année des données en utilisant le dernier élément de la série
        last_data_year = f'20{z_content.iloc[-1][-2:]}'

    # Retourner la série et la dernière année des données
    return [z_content, last_data_year]


def vars_compare(filename, source, rentree):
    from config_path import PATH  
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        pf = ParquetFile(z.open(f'parquet_origine/{filename}.parquet')) 
        first_one_row = next(pf.iter_batches(batch_size = 1)) 
        df = pa.Table.from_batches([first_one_row]).to_pandas() 

        slist_without_s = [i[1:] for i in df.columns[df.columns.str.startswith('S')]]
        slist_to_delete = [f'S{i}' for i in slist_without_s if i in df.columns]
        # print(f"vars commençant par un S à suppr:\n{list(set(slist_to_delete))}")

    # PROGRAMME DE COMPARAISON DE VARIABLES A TERMINER A LA PROCHAINE ACTUALISATION
    return df.drop(columns=slist_to_delete).T.reset_index().assign(source=source, rentree=rentree).rename(columns={0:'ex', 'index':'variable'})


def correctif_vars(df):
    for col in list(df.columns):
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(str)
    
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
            df[col] = df[col].str.replace('nan', '', regex=False).str.strip()
            # df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()
    return df


def src_load(filename, source, rentree):
    from config_path import PATH
    from utils.constants import get_vars
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'parquet_origine/{filename}.parquet'), engine='pyarrow')

    # list columns and lowercase name, create vars RENTREE/SOURCE
    df_vars = df[df.columns[df.columns.isin(get_vars)]]
    df_vars.columns = df_vars.columns.str.lower()
    df_vars = df_vars.assign(rentree=rentree, source=source)
    df_vars = correctif_vars(df_vars)
    return df_vars


def data_save(rentree, df_all, last_data_year):
    from config_path import PATH
    if not os.path.exists(f'{PATH}/output'):
        print("folder OUTPUT creates into DATA_PATH")
    # Create a new directory because it does not exist
        os.mkdir(f'{PATH}/output')
        
    parquet_name = f'sise{str(rentree)[2:4]}.parquet'
    df_all.to_parquet(parquet_name, compression='gzip')

    print(f"Creating the parquet-files by year {parquet_name} into zip in OUTPUT")
    zip_path = os.path.join(PATH, f"output/sise_parquet_{last_data_year}.zip")
    with zipfile.ZipFile(zip_path, 'a') as z:
        z.write(parquet_name)
        
    # Delete the parquet file after adding it to the ZIP
    try:
        os.remove(parquet_name)
        print(f"Deleted: {parquet_name}")
    except Exception as e:
        print(f"Error deleting {parquet_name}: {e}")