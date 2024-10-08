from config_path import DATA_PATH
import pandas as pd
from f1_get_sources import get_sources
from p2_data_parquet import data_load, data_save
from p0_sise_content import zip_content
from exe_init import excel_path

_, last_data_year = zip_content()
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

with pd.ExcelWriter(excel_path, mode='a', if_sheet_exists="replace") as writer:
    pd.DataFrame(df_items.to_excel(writer, sheet_name='items_count_by_vars', index=False, na_rep='Nan'))    