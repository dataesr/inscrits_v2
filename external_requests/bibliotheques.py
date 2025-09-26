import os, pandas as pd
from config_path import PATH
from utils.functions_shared import get_individual_source
from reference_data.ref_data_utils import CORRECTIFS_dict

zip_path=os.path.join(PATH, f"output/sise_cleaned_2024.zip")

c_etab = pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])
disc = pd.DataFrame(CORRECTIFS_dict['DISCIPLINES_SISE'])

df_all = pd.DataFrame()
for rentree in range(2022, 2025):

    df = get_individual_source(zip_path,'sise', rentree)
    
    tmp = df.loc[df.inspr=='O', ['rentree', 'id_paysage', 'cursus_lmd', 'sectdis', 'effectif']]
    tmp = tmp.merge(c_etab[['id_paysage', 'uo_lib', 'id_paysage_actuel', 'anciens_codes_uai']], how='inner', on='id_paysage')
    tmp = (tmp.merge(c_etab[['id_paysage', 'uo_lib']]
                     .drop_duplicates()
                     .rename(columns={'id_paysage':'id_paysage_actuel', 'uo_lib':'uo_lib_actuel'}), 
                     how='left', on='id_paysage_actuel'))

    tmp = (tmp.groupby(list(set(tmp.columns).difference(set(['effectif']))), dropna=False)
            .agg({'effectif':'sum'})
            .reset_index()
        )
    
    tmp = tmp.merge(disc[['discipli', 'discipli_lib', 'sectdis', 'sectdis_lib']], how='left', on='sectdis')
    df_all = pd.concat([df_all, tmp], ignore_index=True)
    df_all = df_all.reindex(columns=['rentree', 
                                    'id_paysage', 
                                    'anciens_codes_uai', 
                                    'uo_lib', 
                                    'id_paysage_actuel', 
                                    'uo_lib_actuel', 
                                    'cursus_lmd', 
                                    'sectdis',
                                    'sectdis_lib',
                                    'discipli',
                                    'discipli_lib',
                                    'effectif'])
    
    df_all.to_excel(f"{PATH}work/bibliotheques_2025_09.xlsx", na_rep='', index=False)