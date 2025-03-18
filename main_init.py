
import os
import pandas as pd
from main_library import *
from config_path import PATH

# liste des datasets du zip parquet, extraction de la dernière années des données dispo
dataset_list, last_data_year = zip_content()
print(f"dernière rentrée de sise dispo:{last_data_year}")

# génération file excel DATA_REVIEW_lastYear
excel_path=data_review_excel()
if not os.path.exists(excel_path):
    with pd.ExcelWriter(excel_path, mode='w', engine='openpyxl' ) as writer:  
        dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)
else:
    with pd.ExcelWriter(excel_path, mode="a", if_sheet_exists="replace", engine='openpyxl' ) as writer:  
        dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)

#######################################
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
        df = vars_compare(filename, source, rentree)
        vars_review = pd.concat([vars_review, df], ignore_index=True)

        df = data_load(filename, source, rentree)
        df_all = pd.concat([df_all, df], ignore_index=True)

    data_save(rentree, df_all, last_data_year)
    for i in df_all.columns.difference(['rentree', 'source']):
        tmp = df_all.groupby(['rentree', 'source'])[i].value_counts(dropna=False).reset_index().rename(columns={i:'item'}).assign(variable=i)
        df_items = pd.concat([df_items, tmp])


with pd.ExcelWriter(excel_path, mode='a', if_sheet_exists="replace") as writer:  
    vars_review.to_excel(writer, sheet_name='l2_vars_source_year', index=False)

df_items.mask(df_items=='', inplace=True)
df_items.to_pickle(f"{PATH}/output/items_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
