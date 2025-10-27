from config_path import PATH
import numpy as np
from utils.functions_shared import *
from reference_data.ref_data_utils import CORRECTIFS_dict

def opendata_first(df):
    va=cols_selected['od_variables_init']
    vn=cols_selected['od_vars_num_init']
    tmp = df[va].groupby(list(set(va).difference(set(vn))), dropna=False)[vn].sum().reset_index()

    for i in ['ue_27', 'ocde_membres', 'bologne', 'brics']:
        tmp[f"attrac_intern_{i}"] = np.where(tmp[i]==1, tmp.eff_ss_cpge, 0)

    tmp['sexe'] = np.where(tmp.sexe==1, 'M', 'F')

    tmp['avance_retard'] = tmp['avance'].str.strip() + tmp['retard'].str.strip()

    tmp['annee'] = tmp['rentree'] + 1
    tmp['annee_universitaire'] = tmp['rentree'].astype(str).str.strip() + '-' + tmp['annee'].astype(str).str.strip().str[-2:]

    tmp = tmp.rename(columns={  'niveaubis':'niveau',
                                'nation_vrai':'attrac_intern',
                                'mobintern':'mobilite_intern',
                                'comui':'com_ins',
                                'cometa':'com_etab',
                                'nbach':'nouv_bachelier',
                                'efft':'effectif_total',
                                'efft_ss_cpge':'effectif_total_sans_cpge',
                                'eff_ss_cpge':'effectif_sans_cpge',
                                'nbach_ss_cpge':'nouv_bachelier_sans_cpge'
                            })


   
    va=cols_selected['od_variables_first']
    vn=cols_selected['od_vars_num']
    tmp = tmp.groupby(va, dropna=False)[vn].sum().reset_index()
    print(f"- size tmp after aggregation : {len(tmp)}")


    etab = pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])[
                        ['id_paysage', 'uo_lib', 'type', 'typologie_d_universites_et_assimiles', 'anciens_codes_uai', 
                        'identifiant_wikidata', 'identifiant_ror', 'operateur_lolf_150', 'id_paysage_actuel', 'secteur']]

    print("- tmp merge etab = tmp1")
    tmp1 = (tmp.merge(etab.loc[etab.secteur=='Public'], how='left', on='id_paysage')
                .rename(columns={
                    'id_paysage':'etablissement_id_paysage',
                    'etabli_ori_uai':'etablissement_id_uai_source',
                    'uo_lib':'etablissement_lib',
                    'type':'etablissement_type',
                    'typologie_d_universites_et_assimiles':'etablissement_typologie', 
                    'anciens_codes_uai':'etablissement_id_uai', 
                    'identifiant_wikidata':'etablissement_id_wikidata', 
                    'identifiant_ror':'etablissement_id_ror', 
                    'id_paysage_actuel':'etablissement_id_paysage_actuel',
                    'id_paysage_epe':'etablissement_compos_id_paysage',
                    'id_paysage_formens':'form_ens_id_paysage'})
    )

    print(f"- size tmp1 filter on etab public: {len(tmp1)}")
    df_size_ori=len(tmp1)

    name_dict={ 'etablissement_id_paysage_actuel':'etablissement_actuel_lib',
                'etablissement_compos_id_paysage':'etablissement_compos_lib',
                'form_ens_id_paysage':'form_ens_lib'}

    for id_col, lib_col in name_dict.items():
        tmp1[id_col] = tmp1[id_col].fillna('')
        tmp1 = (tmp1.merge(etab[['id_paysage', 'uo_lib']].rename(columns={'id_paysage':id_col, 'uo_lib':lib_col}), 
                            how='left', on=id_col)
    )
    no_same_size(df_size_ori, tmp1)

    # les communes
    print("- TMP1 merge communes")
    communes = pd.DataFrame(CORRECTIFS_dict['LES_COMMUNES'])[
                            ['com_code', 'com_nom', 'uucr_id', 'uucr_nom', 'dep_id', 'dep_nom',
                            'aca_id', 'aca_nom', 'reg_id', 'reg_nom']]


    for i in ['com_etab', 'com_ins']:
        if i=='com_etab':
            pref='etablissement_'
        elif i=='com_ins':
            pref='implantation_'

        name_new_dict={
        'com_code':f'{pref}code_commune', 'com_nom':f'{pref}commune', 'uucr_id':f'{pref}id_uucr', 'uucr_nom':f'{pref}uucr', 'dep_id':f'{pref}id_departement', 
        'dep_nom':f'{pref}departement', 'aca_id':f'{pref}id_academie', 'aca_nom':f'{pref}academie', 'reg_id':f'{pref}id_region', 'reg_nom':f'{pref}region'}

    
        tmp1 = (tmp1.merge(communes, how='left', left_on=i, right_on='com_code')
                    .rename(columns=name_new_dict)
                    .drop(columns=i))
    no_same_size(df_size_ori, tmp1)

    numeric_cols = tmp1.select_dtypes(include=['number']).columns
    tmp1[numeric_cols] = tmp1[numeric_cols].astype('Int64')


    character_cols = tmp1.select_dtypes(include=['object', 'string']).columns
    tmp1[character_cols] = tmp1[character_cols].fillna('')


    tmp1 = tmp1.assign(etablissement_localisation=tmp1['etablissement_region'], implantation_localisation=tmp1['implantation_region'])
    for i in ['etablissement_', 'implantation_']:
        tmp1.loc[tmp1[f"{i}region"]!=tmp1[f"{i}academie"], f"{i}localisation"] = tmp1[f"{i}localisation"] + ">" + tmp1[f"{i}academie"]
        tmp1.loc[tmp1[f"{i}academie"]!=tmp1[f"{i}departement"], f"{i}localisation"] = tmp1[f"{i}localisation"] + ">" + tmp1[f"{i}departement"]
        tmp1.loc[tmp1[f"{i}departement"]!=tmp1[f"{i}uucr"], f"{i}localisation"] = tmp1[f"{i}localisation"] + ">" + tmp1[f"{i}uucr"]
        tmp1.loc[tmp1[f"{i}uucr"]!=tmp1[f"{i}commune"], f"{i}localisation"] = tmp1[f"{i}localisation"] + ">" + tmp1[f"{i}commune"]

    return tmp1

def od_first_enrich(df):

    refs = ['SEXE', 'BAC_RGRP', 'AVANCE_RETARD', 'PROXBAC', 'PROXREGBAC', 'ATTRAC_INTERN', 'MOBILITE_INTERN', 'DNDU', 
           'CURSUS_LMD', 'LMDDONTBIS', 'NIVEAU', 'DEGETU', 'SECTDIS', 'SPECIUT']

    for ref in refs:
        if ref=='SECTDIS':
            s=pd.DataFrame(CORRECTIFS_dict['SECTDIS']).drop(columns=['gd_discipline2', 'gd_discipline2_lib'])
        elif ref=='SPECIUT':
            s=pd.DataFrame(CORRECTIFS_dict['SPECIUT']).drop(columns=['spec_iut_rgp'])
        elif ref=='AVANCE_RETARD':
            s=pd.DataFrame(CORRECTIFS_dict['AVANCE_RETARD']).drop(columns=['avance', 'retard'])
        else:
            s = pd.DataFrame(CORRECTIFS_dict[ref])

        rkey=ref.lower()
        df = pd.merge(df, s, how='left', on=rkey)
    return df
