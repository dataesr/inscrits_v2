from config_path import *
import pandas as pd
from project.P1_data_parquet import data_load, data_save
from P0_sise_content import get_sources, zip_content

last_data_year = zip_content()[1]
print(f"dernière rentrée de sise dispo:{last_data_year}")

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
# ALL_RENTREES = list(range(2004, 2005))

df_items = pd.DataFrame()

for rentree in ALL_RENTREES:
    sources = get_sources(rentree)
    df_all = pd.DataFrame()
    for source in sources:
        filename = f'{source}{str(rentree)[2:4]}'
        print(filename)
        df = data_load(filename, source, rentree)
        df_all = pd.concat([df_all, df], ignore_index=True)

    data_save(rentree, df_all, last_data_year)

    for i in df_all.columns.difference(['rentree', 'source']):
        tmp = df_all.groupby(['rentree', 'source'])[i].value_counts(dropna=False).reset_index().rename(columns={i:'item'}).assign(variable=i)
        df_items = pd.concat([df_items, tmp])

# with pd.ExcelWriter(data_review_excel(), mode='a', if_sheet_exists="replace") as writer:
#     pd.DataFrame(df_items.to_excel(writer, sheet_name='items_count_by_vars', index=False, na_rep='Nan')) 
df_items.mask(df_items=='', inplace=True)
df_items.to_pickle(f"{DATA_PATH}/output/items_by_vars{last_data_year}.pkl",compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
