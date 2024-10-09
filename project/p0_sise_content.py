from config_path import DATA_PATH
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
    with zipfile.ZipFile(f"{DATA_PATH}input/parquet_origine.zip", 'r') as z:
        z_content=pd.Series(filter(None, [i.split('.')[0].split('/')[1] for i in z.namelist()]), name='dataset_name')
        last_data_year = f'20{z_content.iloc[-1][-2:]}'
    return [z_content, last_data_year]


def vars_compare(filename, source, rentree):    
    with zipfile.ZipFile(f"{DATA_PATH}input/parquet_origine.zip", 'r') as z:
        pf = ParquetFile(z.open(f'parquet_origine/{filename}.parquet')) 
        first_one_row = next(pf.iter_batches(batch_size = 1)) 
        df = pa.Table.from_batches([first_one_row]).to_pandas() 

        slist_without_s = [i[1:] for i in df.columns[df.columns.str.startswith('S')]]
        slist_to_delete = [f'S{i}' for i in slist_without_s if i in df.columns]
        # print(f"vars commençant par un S à suppr:\n{list(set(slist_to_delete))}")

    # PROGRAMME DE COMPARAISON DE VARIABLES A TERMINER A LA PROCHAINE ACTUALISATION
    return df.drop(columns=slist_to_delete).T.reset_index().assign(source=source, rentree=rentree).rename(columns={0:'ex', 'index':'variable'})

def data_review_excel():
    last_data_year = zip_content()[1]
    return os.path.join(DATA_PATH, f"data_review_{last_data_year}.xlsx")