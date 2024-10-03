def  zip_content():
    from config_path import DATA_PATH
    import pandas as pd, zipfile

    with zipfile.ZipFile(f"{DATA_PATH}input/parquet_origine.zip", 'r') as z:
        z_content=pd.Series(filter(None, [i.split('.')[0].split('/')[1] for i in z.namelist()]), name='dataset_name')
        last_data_year = f'20{z_content.iloc[-1][-2:]}'
    return z_content, last_data_year


def vars_compare(filename, source, rentree):
    from config_path import DATA_PATH
    import pandas as pd, zipfile
    import pyarrow as pa
    from pyarrow.parquet import ParquetFile
    
    with zipfile.ZipFile(f"{DATA_PATH}input/parquet_origine.zip", 'r') as z:
        
        pf = ParquetFile(z.open(f'parquet_origine/{filename}.parquet')) 
        first_one_row = next(pf.iter_batches(batch_size = 1)) 
        df = pa.Table.from_batches([first_one_row]).to_pandas() 

        slist_without_s = [i[1:] for i in df.columns[df.columns.str.startswith('S')]]
        slist_to_delete = [f'S{i}' for i in slist_without_s if i in df.columns]
        print(f"vars commençant par un S à suppr:\n{list(set(slist_to_delete))}")

    # PROGRAMME DE COMPARAISON DE VARIABLES A TERMINER A LA PROCHAINE ACTUALISATION
    return df.drop(columns=slist_to_delete).T.reset_index().assign(source=source, rentree=rentree).rename(columns={0:'ex', 'index':'variable'})
   