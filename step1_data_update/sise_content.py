
import pandas as pd, zipfile, os
import pyarrow as pa
from pyarrow.parquet import ParquetFile

def get_sources(annee):
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


def  zip_content():
    from config_path import PATH
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        z_content=pd.Series(filter(None, [i.split('.')[0].split('/')[1] for i in z.namelist()]), name='dataset_name')
        last_data_year = f'20{z_content.iloc[-1][-2:]}'
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

def data_review_excel():
    from config_path import PATH
    last_data_year = zip_content()[1]
    return os.path.join(PATH, f"data_review_{last_data_year}.xlsx")

def data_load(filename, source, rentree):
    from config_path import PATH
    from utils import vars_list
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'parquet_origine/{filename}.parquet'), engine='pyarrow')

    # list columns and lowercase name, create vars RENTREE/SOURCE
    df_vars = df[df.columns[df.columns.isin(vars_list)]]
    df_vars.columns = df_vars.columns.str.lower()
    df_vars = df_vars.assign(rentree=rentree, source=source)

    # to check the data from the new datasets
    with pd.ExcelWriter(data_review_excel(), mode='a', if_sheet_exists="replace") as writer:  
        pd.DataFrame({"name": df_vars.columns, "non-nulls": len(df_vars)-df_vars.isnull().sum().values, "nulls": df_vars.isnull().sum().values}).to_excel(writer, sheet_name=filename, index=False)
    
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
    zip_path = os.path.join(PATH, f"output/parquet_basic_{last_data_year}.zip")
    with zipfile.ZipFile(zip_path, 'a') as z:
        z.write(parquet_name)
        
    del parquet_name