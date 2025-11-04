from config_path import PATH
import numpy as np, pandas as pd
from utils.functions_shared import *
from reference_data.ref_data_utils import CORRECTIFS_dict

def tableau_adjust(df, va, vn):
    df = df.assign(
                localisation = np.where(df['implantation_id_uucr']=="UU00851", df['implantation_code_commune'], df['implantation_id_uucr']),
                sexe = np.where(df['sexe']=="M", "1", "2")
                            )

    ar_dict = {item['bac_age']:item['avance_retard'] for item in CORRECTIFS_dict['AVANCE_RETARD']}
    df['bac_age'] = df['bac_age'].replace(ar_dict)

    df = (df.groupby(va, dropna=False)[vn]
           .sum()
           .reset_index())
    
    df = (df.rename(columns={
            "etablissement_id_paysage":"etabli",
            "implantation_id_departement":"depins",
            "bac":"bac_rgrp",
            "attrac_intern":"nation_vrai",
            "sect_disciplinaire":"sectdis",
            "discipline":"discipli",
            "gd_discipline":"gddisc",
            "eff_dei":"effectif_dei"
            })
        )
    return df

def od_tableau(df):
    # sas opendata19 lignes 414
    va = cols_selected['tableau_vars_short']
    vn = list(set(cols_selected['od_vars_num']) - {'efft', 'efft_ss_cpge'})
    tableau = df.loc[(df.rentree > 2017)&(df.operateur_lolf_150=='O')]

    # tableau 2
    tableau2 = tableau_adjust(tableau, va, vn)
    path_export= f'{PATH}opendata/tableau2.txt'
    tableau2.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)
    # tableau2.to_pickle(path_export, compression='gzip')

    # tableau 1
    # sas opendata lignes 321 -> 411
    va = [item for item in va if item not in {'optiut', 'parcoursbut'}]
    tableau1 = tableau_adjust(tableau, va, vn)
    path_export= f'{PATH}opendata/tableau.txt'
    tableau1.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)
    # tableau1.to_pickle(path_export, compression='gzip')


    uo = tableau.etablissement_id_paysage.value_counts().reset_index(name='freq')
    uo = (pd.merge(uo, 
                pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])[
                ['id_paysage', 'type', 'uo_lib', 'uucr_id', 'uucr_nom', 'com_code', 'com_nom', 
                 'dep_id', 'dep_nom', 'aca_id', 'aca_nom', 'reg_id', 'reg_nom', 'coordonnees']],
                 how='left', left_on='etablissement_id_paysage', right_on='id_paysage')
            .rename(columns={"etablissement_id_paysage":"uo",
                            "type":"uo_type",
                            "uo_lib":"uo_lib",
                            "uucr_id":"ui_siege_id",
                            "uucr_nom":"uo_siege",
                            "dep_id":"uo_dep_id",
                            "dep_nom":"uo_dep_nom",
                            "aca_id":"uo_aca_id",
                            "aca_nom":"uo_aca_nom",
                            "reg_id":"uo_reg_id",
                            "reg_nom":"uo_reg_nom"})    
            )

    uo[["long", "lat"]] = uo["coordonnees"].str.split(",", expand=True)
    uo = (uo.assign(rers2015='oui',
                   uo_siege=np.where(uo.ui_siege_id=='UU00851', uo.com_nom, uo.uo_siege),
                   ui_siege_id=np.where(uo.ui_siege_id=='UU00851', uo.com_code, uo.ui_siege_id)
                   )
            .drop(columns=['com_code', 'com_nom', 'id_paysage', 'coordonnees'])
            )
 
    path_export= f'{PATH}opendata/uo.txt'.encode('utf-8').decode('utf-8')
    uo.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)
    # uo.to_pickle(path_export, compression='gzip')