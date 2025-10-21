
import pandas as pd, zipfile, os, json
import pyarrow as pa
from pyarrow.parquet import ParquetFile

def get_most_recent_year(items):
    # Extraire les années potentielles et les convertir en entiers
    years = []
    for item in items:
        # Prendre les deux derniers caractères et ajouter le préfixe '20'
        year_str = '20' + item[-2:]
        if year_str.isdigit():  # Vérifier si c'est bien un nombre
            years.append(int(year_str))
    years.remove(2099)
    return max(years)


def zip_content():
    # Importer le chemin de configuration depuis un module externe
    from config_path import PATH
    from utils.functions_shared import last_file_into_folder

    zip_file_path=last_file_into_folder(f"{PATH}input/", 'zip', 'parquet_origine')

    # Ouvrir le fichier ZIP en mode lecture
    parquet_files = []

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.endswith('.parquet'):
                # Extraire uniquement le nom du fichier sans le chemin
                file_name = os.path.basename(file_info.filename)
                parquet_files.append(file_name.split('.')[0] )

        # Extraire la dernière année des données en utilisant le dernier élément de la série
        last_data_year = get_most_recent_year(parquet_files)

    # Retourner la série et la dernière année des données
    return [parquet_files, last_data_year]


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


def vars_init(df):
    from modules.cleansing import delete
    CONF=json.load(open('utils/config_sise.json', 'r'))
    for conf in CONF:
        var_sise = conf["var_sise"]
        format_type = conf["format"]
        missing_value = conf["missing_value"]

        if var_sise in df.columns:
            if format_type=='str':
                df[var_sise] = df[var_sise].astype(format_type)
                df[var_sise] = df[var_sise].str.split('.0', regex=False).str[0].str.strip()
                df.loc[df[var_sise].str.match(pat='(nan)|(none)', case=False), var_sise] = ''        
                df = df.mask(df=='')
                df[var_sise] = df[var_sise].fillna(missing_value)

            if format_type=='int':
                df[var_sise] = pd.to_numeric(df[var_sise], errors='coerce').astype('Int64')
                df[var_sise] = df[var_sise].fillna(missing_value)
        
        else:
            print(f"- add {var_sise} -> missing value {missing_value}")
            df[var_sise] = missing_value

    df = delete(df)
            
    return df


def src_load(last_data_year, filename, source, rentree):
    from config_path import PATH
    with zipfile.ZipFile(f"{PATH}input/parquet_origine_{last_data_year}.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'{filename}.parquet'), engine='pyarrow')

    CONF=json.load(open('utils/config_sise.json', 'r'))

    # list columns and lowercase name, create vars RENTREE/SOURCE
    df_vars = df[df.columns[df.columns.str.lower().isin([conf.get('var_sise') for conf in CONF if conf.get('var_sise')])]]
    df_vars.columns = df_vars.columns.str.lower()
    df_vars = df_vars.assign(rentree=rentree, source=source)
    return df_vars

def rattach_init(rentree):
    import pyreadstat
    from config_path import PATH
    PATH_FORMAT=f"{PATH}format/"
    data_rattach = []
    if rentree in range(2001, 2023):
        # lecture format
        df_format, meta_format = pyreadstat.read_sas7bcat(f'{PATH_FORMAT}inscri{str(rentree)[2:4]}/formats.sas7bcat',
                                                        encoding='iso-8859-1')
        for compos in meta_format.value_labels['$RATTACH']:
            rattach = meta_format.value_labels['$RATTACH'][compos]
            data_rattach.append({'rentree': rentree, 'compos': compos, 'rattach': rattach})
    if rentree in range(2023, int(rentree)+1):
        df = pd.read_parquet(f'{PATH_FORMAT}inscri{str(rentree)[2:4]}/bce_a24.parquet', engine='pyarrow')
        for index, row in df.iterrows():
            data_rattach.append({'rentree': rentree, 'compos': row['numero_uai'], 'rattach': row['composante_rattachement']})
    df_rattach = pd.DataFrame(data_rattach)

    return df_rattach

def data_save(rentree, df_all, last_data_year):
    from config_path import PATH
    if not os.path.exists(f'{PATH}output'):
        print("folder OUTPUT creates into DATA_PATH")
    # Create a new directory because it does not exist
        os.mkdir(f'{PATH}output')
        
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