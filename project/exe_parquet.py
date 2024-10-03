from config_path import DATA_PATH
import pandas as pd
from f1_get_sources import get_sources
from p2_data_parquet import data_load, data_save
from p0_sise_content import zip_content


_, last_data_year = zip_content()
print(f"dernière rentrée de sise dispo:{last_data_year}")

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
# ALL_RENTREES = list(range(2004, 2006))

for rentree in ALL_RENTREES:
    sources = get_sources(rentree)
    df_all = pd.DataFrame()
    for source in sources:
        filename = f'{source}{str(rentree)[2:4]}'
        print(filename)
        df = data_load(filename, source, rentree, last_data_year)
        df_all = pd.concat([df_all, df], ignore_index=True)

    data_save(rentree, df_all, last_data_year) 