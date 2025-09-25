import os, pandas as pd
from config_path import PATH
from utils.functions_shared import get_individual_source
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN

zip_path=os.path.join(PATH, f"output/sise_cleaned_2024.zip")

c_etab = pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])
disc = pd.DataFrame(BCN['N_DISCIPLINE_SISE']).rename(columns={'libelle_court':'discipli_libelle'})
df_all = pd.DataFrame()

for rentree in range(2022, 2025):

    df = get_individual_source(zip_path,'sise', rentree)
    
    tmp = df.loc[df.inspr=='O', ['rentree', 'id_paysage', 'cursus_lmd', 'sectdis', 'discipli', 'effectif']]
    tmp = tmp.merge(c_etab[['id_paysage', 'uo_lib', 'id_paysage_2025', 'anciens_codes_uai']], how='inner', on='id_paysage')
    tmp = tmp.merge(c_etab[['id_paysage_2025', 'uo_lib_courant']].drop_duplicates(), how='left', on='id_paysage_2025')

    tmp = (tmp.groupby(list(set(tmp.columns).difference(set(['effectif']))), dropna=False)
            .agg({'effectif':'sum'})
            .reset_index()
        )
    
    tmp = tmp.merge(disc[['discipli', 'discipli_libelle']], how='left', on='discipli')
    df_all = pd.concat([df_all, tmp], ignore_index=True)
    df_all = df_all.reindex(columns=['rentree', 
                                    'id_paysage', 
                                    'uo_lib', 
                                    'id_paysage_2025', 
                                    'uo_lib_courant', 
                                    'anciens_codes_uai', 
                                    'cursus_lmd', 
                                    'sectdis',
                                    'discipli',
                                    'discipli_libelle',
                                    'effectif'])