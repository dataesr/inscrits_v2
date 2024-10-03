from config_path import *
import os
import pandas as pd
from f1_get_sources import get_sources
from p0_sise_content import vars_compare, zip_content


# liste des datasets du zip parquet, extraction de la dernière années des données dispo
dataset_list, last_data_year = zip_content()

file_path=os.path.join(DATA_PATH, f"data_review_{last_data_year}.xlsx")
if not os.path.exists(file_path):
    with pd.ExcelWriter(file_path, mode='w', engine='openpyxl' ) as writer:  
        dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)
else:
    with pd.ExcelWriter(file_path, mode="a", if_sheet_exists="replace", engine='openpyxl' ) as writer:  
        dataset_list.to_excel(writer, sheet_name='l1_datasets', index=False)

# etat des variables par année source
vars_review = pd.DataFrame(columns=['variable', 'ex', 'source', 'rentree'])

ALL_RENTREES = list(range(2004, int(last_data_year)+1))
for rentree in ALL_RENTREES:
    sources = get_sources(rentree)
    for source in sources:

        filename = f'{source}{str(rentree)[2:4]}'
        print(filename)
        df = vars_compare(filename, source, rentree)

        vars_review = pd.concat([vars_review, df], ignore_index=True)

with pd.ExcelWriter(file_path, mode='a', if_sheet_exists="replace") as writer:  
    vars_review.to_excel(writer, sheet_name='l2_vars_source_year', index=False)