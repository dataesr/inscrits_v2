def data_load(filename, source, rentree, last_data_year):
    import zipfile
    import pandas as pd
    from pyarrow.parquet import ParquetFile
    from config_path import DATA_PATH
    from project.utils import vars_list

    with zipfile.ZipFile(f"{DATA_PATH}input/parquet_origine.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'parquet_origine/{filename}.parquet'), engine='pyarrow')

    df_vars = df[df.columns[df.columns.isin(vars_list)]]
    df_vars.columns = df_vars.columns.str.lower()
    df_vars = df_vars.assign(rentree=rentree, source=source)

    with pd.ExcelWriter(f"{DATA_PATH}data_review_{last_data_year}.xlsx", mode='a', if_sheet_exists="replace") as writer:  
        pd.DataFrame({"name": df_vars.columns, "non-nulls": len(df_vars)-df_vars.isnull().sum().values, "nulls": df_vars.isnull().sum().values}).to_excel(writer, sheet_name=filename, index=False)
    
    return df_vars