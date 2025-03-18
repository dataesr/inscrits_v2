import os
import zipfile
import pandas as pd
from config_path import *
from utils import vars_list
from P0_sise_content import data_review_excel

def data_load(filename, source, rentree):
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'parquet_origine/{filename}.parquet'), engine='pyarrow')

    df_vars = df[df.columns[df.columns.isin(vars_list)]]
    df_vars.columns = df_vars.columns.str.lower()
    df_vars = df_vars.assign(rentree=rentree, source=source)

    with pd.ExcelWriter(data_review_excel(), mode='a', if_sheet_exists="replace") as writer:  
        pd.DataFrame({"name": df_vars.columns, "non-nulls": len(df_vars)-df_vars.isnull().sum().values, "nulls": df_vars.isnull().sum().values}).to_excel(writer, sheet_name=filename, index=False)
    
    return df_vars


def data_save(rentree, df_all, last_data_year):

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