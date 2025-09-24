import os, pandas as pd
from config_path import PATH
from utils.functions_shared import get_individual_source
from reference_data.ref_data_utils import CORRECTIFS_dict

zip_path=os.path.join(PATH, f"output/sise_cleaned_2024.zip")

c_etab = pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])
df_all = pd.DataFrame()

for rentree in range(2022, 2025):

    df = get_individual_source(zip_path,'sise', rentree)
    
    tmp = df.loc[df.inspr=='O', ['rentree', 'etabli', 'id_paysage', 'cursus_lmd', 'discipli', 'effectif']]
    tmp = tmp.merge(c_etab[['id_paysage', 'nom_court']], how='inner', on='id_paysage')
    tmp = tmp.groupby(['rentree', 'etabli', 'cursus_lmd', 'discipli']).agg({'effectif':'sum'}).reset_index()

    df_all = pd.concat([df_all, tmp], ignore_index=True)