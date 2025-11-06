from config_path import PATH
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
from utils.functions_shared import replace_by_nan, work_csv
import json, pandas as pd, numpy as np


######################################################################################
# ETABISSEMENTS
#####################################################################################
def from_uai_to_paysage(etab): 
    a_uai = pd.DataFrame(CORRECTIFS_dict['A_UAI'])
    a_uai['rentree'] = a_uai['rentree'].astype(int)
    tmp = a_uai.loc[a_uai.type=='inscri', ['rentree', 'source', 'etabli', 'id_paysage']].drop_duplicates()
    tmp['paysage_count'] = (tmp.groupby(['rentree', 'source', 'etabli'], dropna=False)['id_paysage']
                               .transform('count')
                            )
    if len(tmp.query('paysage_count>1'))>0:
        print(f"- ATTENTION ++ id_paysage for etabli_uai: {tmp.query('paysage_count>1')}")

    etab = etab.merge(tmp, how='left', on=['rentree', 'source', 'etabli'])

    id_paysage_null = etab.loc[etab.id_paysage.isnull(), ['rentree', 'source', 'etabli', 'lib_etabli', 'etabli_valide']].drop_duplicates()
    paysage_id = pd.DataFrame(PAYSAGE_dict['paysage_id'])
    id_paysage_null = id_paysage_null.merge(paysage_id.loc[paysage_id.id_type=='uai'], how='left', left_on='etabli', right_on='id_value')[
        ['rentree', 'source', 'etabli', 'etabli_valide', 'lib_etabli', 'id_paysage',
        'usualname', 'active', 'id_startdate', 'id_enddate']]
    if len(id_paysage_null)>0:
        print("- ATTENTION, create file uai_paysage_missing_into_A_UAI for updating A_UAI")
        work_csv(id_paysage_null, 'uai_paysage_missing_into_A_UAI')

    return etab.drop(columns='paysage_count').drop_duplicates()

def from_id_to_lib(etab):
    c_etab = pd.DataFrame([{'id_paysage': i['id_paysage'], 'lib_paysage': i['uo_lib_courant']} for i in CORRECTIFS_dict['C_ETABLISSEMENTS']]).drop_duplicates()
    c_etab['paysage_count'] = c_etab.groupby(['id_paysage'], dropna=False)['lib_paysage'].transform('count')
    if len(c_etab.query('paysage_count>1'))>0:
        print(f"- ATTENTION ++ lib_paysage for id_paysage: {c_etab.query('paysage_count>1')}")
        
    etab = etab.merge(c_etab, how='left', on='id_paysage')
    return etab.drop(columns='paysage_count').drop_duplicates()

def d_epe_enrich(etab):
    epe =  pd.DataFrame(CORRECTIFS_dict['D_EPE']).assign(rentree=lambda x: x['rentree'].astype(int))
    etab = (etab
            .assign(id_paysage_source=etab['id_paysage'])
            .merge(epe[['rentree', 'id_paysage', 'id_paysage_epe']], on=['rentree', 'id_paysage'], how='left')
                .drop_duplicates())
    
    etab.loc[~etab['id_paysage_epe'].isnull(), 'id_paysage_epe_etab_compos'] = etab.loc[~etab['id_paysage_epe'].isnull(), 'id_paysage']
    etab.loc[~etab['id_paysage_epe'].isnull(), 'id_paysage'] = etab.loc[~etab['id_paysage_epe'].isnull(), 'id_paysage_epe']

    return etab.drop_duplicates()

def iut_enrich(etab):
    iut =  (pd.DataFrame(CORRECTIFS_dict['M_IUT'])
            .assign(rentree=lambda x: x['rentree'].astype(int))
            .rename(columns={'ur':'rattach', 'ui':'compos'}))
    etab = (etab.merge(iut[['rentree', 'rattach', 'compos', 'id_paysage_iut', 'id_paysage_iut_campus', 'id_paysage_iut_pole']],
                how='left', on=['rentree', 'rattach', 'compos'])
    )
    return etab.drop_duplicates()

def ing_enrich(etab):
    ing =  (pd.DataFrame(CORRECTIFS_dict['N_ING'])
            .assign(rentree=lambda x: x['rentree'].astype(int))
            .rename(columns={'ur':'rattach', 'ui':'compos'}))
    etab = (etab.merge(ing[['rentree', 'rattach', 'compos', 'id_paysage_ing', 'id_paysage_ing_campus']],
                how='left', on=['rentree', 'rattach', 'compos'])
    )
    return etab.drop_duplicates()


#################################################################################################################################
# ETUDIANTS
##################################################################################################################################
def niveau_retard_avance(df):
    print("- niveau_retard_avance")
    mask_typ_dipl = (df.typ_dipl.isin(['AC','XE','CB','XA','XB','XD','DR']))
    df.loc[mask_typ_dipl, 'niveaubis'] = df.loc[mask_typ_dipl, 'niveau']
    df.loc[~mask_typ_dipl, 'niveaubis'] ='XX'

    df.loc[df.anbac!=-1, 'age_bac'] = df.loc[df.anbac!=-1, 'anbac'] - df.loc[df.anbac!=-1, 'annais']

    mask_age_retard = (df.age_bac>18)
    mask_age_avance = (df.age_bac<18)

    back = df.rentree.unique()[0]
    if back < 2012:
        mask_bac_rgp = (df.bac_rgrp.isin(["A", "1", "2", "3", "4", "5", "9"]))
    else:
        mask_bac_rgp = (df.bac_rgrp.isin(["A", "1", "2", "3", "4", "5", "6", "9"]))

    df.loc[mask_bac_rgp & mask_age_retard, 'retard']='O'
    df.loc[mask_bac_rgp & ~mask_age_retard, 'retard']='N'
    df.loc[mask_bac_rgp & mask_age_avance, 'avance']='O'
    df.loc[mask_bac_rgp & ~mask_age_avance, 'avance']='N'

    if back < 2012:
        mask_bac_rgp = (df.bac_rgrp=='6')
        mask_age_retard = (df.age_bac>19)
        mask_age_avance = (df.age_bac<19)
        df.loc[mask_bac_rgp & mask_age_retard, 'retard']='O'
        df.loc[mask_bac_rgp & ~mask_age_retard, 'retard']='N'
        df.loc[mask_bac_rgp & mask_age_avance, 'avance']='O'
        df.loc[mask_bac_rgp & ~mask_age_avance, 'avance']='N'

    df.loc[(df.anbac==-1) | (df.age_bac<10) | (df.bac_rgrp=='7'), 'retard']="X"
    df.loc[(df.anbac==-1) | (df.age_bac<10) | (df.bac_rgrp=='7'), 'avance']="X"

    return df

def dndu_enrich(df):
    print("- DNU enrich")
    df_dndu =  pd.DataFrame(CORRECTIFS_dict['I_DNDU'])
    df = df.merge(df_dndu, how='left', on='typ_dipl')
    df['dndu'] = replace_by_nan(df['dndu'])
    df.loc[df.dndu.isnull(), 'dndu']='DN'
    return df

def lmd_enrich(df):
    print("- LMD enrich")
    df_lmd =  pd.DataFrame(CORRECTIFS_dict['J_LMDDONT'])
    df = pd.merge(df, df_lmd[['typ_dipl', 'lmddont', 'lmddontbis']],
                on='typ_dipl', how='left')

    mask_dipl = (df.diplom.isin(['6001000','6004000','8000010']))
    df.loc[mask_dipl, 'lmddont'] = 'AUTRES'
    df.loc[mask_dipl, 'lmddontbis'] = 'AUTRES'
    
    df.loc[df.typ_dipl=='XA', 'lmddontbis'] = 'LIC_L_AUT'
    df.loc[(df.typ_dipl=='XA') & (df.par_type=='0001291'), 'lmddontbis'] = 'LIC_L_LAS'
    df.loc[(df.typ_dipl=='XA') & (df.par_type=='0001408'), 'lmddontbis'] = 'LIC_L_LSPS'
    df.loc[(df.typ_dipl=='XA') & (df.par_type=='0001487'), 'lmddontbis'] = 'LIC_L_CPES' #classe préparatoire (généraliste) sensibilisation RU
    df.loc[(df.lmddont=='') | (pd.isna(df.lmddont)) | (df.lmddont.isnull()), 'lmddont'] = 'AUTRES'
    df.loc[(df.lmddontbis=='') | (pd.isna(df.lmddontbis)) | (df.lmddontbis.isnull()), 'lmddontbis'] = 'AUTRES'

    return df

def ed_enrich(df):
    print("- ED enrich")
    df_ed =  pd.DataFrame(CORRECTIFS_dict['L_ED'])
    df = pd.merge(df, df_ed[['numed', 'id_paysage_ed']], how='left', on='numed')
    df['id_paysage_ed'] = replace_by_nan(df['id_paysage_ed']) 
    return df

def dutbut_enrich(df):
    df_dutbut =  pd.DataFrame(CORRECTIFS_dict['O_DUTBUT'])
    df = df.merge(df_dutbut[['diplom', 'correspondanceiut', 'speciut', 'optiut', 'parcoursbut']],
                 how='left', on=['diplom'])
    for c in ['correspondanceiut', 'speciut', 'optiut', 'parcoursbut']:
        df[c] = replace_by_nan(df[c])
        df.loc[df[c].isnull(), c] = 'AUTRES'
    return df  

def communes_enrich(df):
    df_communes =  pd.DataFrame(CORRECTIFS_dict['LES_COMMUNES'])
    df_communes['depins'] = df_communes['dep_id'].str[1:4]
    df_communes['uucrins'] = df_communes['uucr_id']
    df_communes['comui'] = df_communes['com_code']
    df = pd.merge(df, df_communes[['depins', 'uucrins', 'comui']], how='left', on='comui')
    
    df_communes['uucretab'] = df_communes['uucr_id']
    df_communes['cometa'] = df_communes['com_code']
    df = pd.merge(df, df_communes[['uucretab', 'cometa']], how='left', on='cometa')
    return df

def prox_enrich(df):
    df_proximite = pd.DataFrame(CORRECTIFS_dict['H_PROXIMITE'])
    df_proximite['depins'] = df_proximite['departement_ui']
    df_proximite['deprespa'] = df_proximite['departement_parents']
    df_proximite['proximite'] = df_proximite['proximit_']
    df_proximite['proxreg'] = df_proximite['prox_r_gions']
    df = pd.merge(df, df_proximite[['depins', 'deprespa', 'proxreg', 'proximite']], how='left' , on=['depins', 'deprespa'])
    
    df_proximite['proxbac'] = df_proximite['proximit_']
    df_proximite['proxregbac'] = df_proximite['prox_r_gions']
    df_proximite['depbac'] = df_proximite['departement_parents']
    df = pd.merge(df, df_proximite[['depins', 'depbac', 'proxbac', 'proxregbac', 'outremer']], how='left', on=['depins', 'depbac'])

    df['depbac'] = replace_by_nan(df.depbac)
    df['proxbac'] = replace_by_nan(df.proxbac)
    df['proxregbac'] = replace_by_nan(df.proxregbac)
    df.loc[df.bac_rgrp=='7', 'proxbac'] = '9 - non-bachelier'
    df.loc[(df.bac_rgrp=='9') & (df.proxbac.isnull()), 'proxbac']= "5 - NR"
    df.loc[(df.depbac.isnull()) & (df.bac_rgrp!='7'), 'proxbac']= "5 - NR"

    df.loc[df.bac_rgrp=='7', 'proxregbac'] = '9 - non-bachelier'
    df.loc[(df.bac_rgrp=='9') & ((df.proxregbac.isnull())), 'proxregbac']= "5 - NR"
    df.loc[(df.depbac.isnull()) & (df.bac_rgrp!='7'), 'proxregbac']= "5 - NR"
    
    df.loc[(df.bac_rgrp=='7'), 'outremer'] = 'non-bachelier'
    return df

def deptoreg(df):
    deptoreg = pd.DataFrame(CORRECTIFS_dict['DEPTOREG'])
    df = df.merge(deptoreg, how='left', left_on='deprespa', right_on='in').drop(columns={'in'}).rename(columns={'out':'regrespa'})
    deptoregnew = pd.DataFrame(CORRECTIFS_dict['DEPTOREGNEW'])
    df = df.merge(deptoregnew, how='left', left_on='deprespa', right_on='in').drop(columns={'in'}).rename(columns={'out':'regrespa16'})
    return df

def country_enrich(df):
    print("- COUNTRY enrich")
    df_pays =  pd.DataFrame(CORRECTIFS_dict['G_PAYS'])
    df_pays['nation'] = df_pays['pays']
    df = df.merge(df_pays[['nation', 'continent', 'ue_28', 'ue_27', 'ue_euro', 'ocde_membres', 'ocde_obs', 'bologne', 'brics']],
                on='nation', how='left')
    df['fr_etr'] = np.where(df.nation=='100', '1', '2')
    for i in ['ue_28', 'ue_27', 'ue_euro', 'ocde_membres', 'ocde_obs', 'bologne', 'brics']:
        df[i] = df[i].astype(pd.Int64Dtype())
        df.loc[(df[i].isnull()) , i] = 0
    return df

def nation_bac_add(df):
    print("- NATION_BAC add")
    mask_bac = df.bac.isin(["0031","0001","0002"])
    df.loc[mask_bac, 'nation_bac'] = "E"
    df.loc[~mask_bac, 'nation_bac'] = "F"
    df.loc[(df.nation_bac=='E')&(df.nation!='100'), 'nation_vrai'] = "E"
    df.loc[(df.nation_bac!='E')|(df.nation=='100'), 'nation_vrai'] = "F"
    return df

def mobilite_add(df):
    print("- MOBILITE add")
    df.loc[(df.nation!='100')&((df.acabac=='00')|(df.bac=='0031')),'mobintern'] = "M"
    df.loc[(df.nation=='100')|((df.acabac!='00')&(df.bac!='0031')),'mobintern'] = "X"
    return df

def effectifs(df):
    df.loc[(df.inspr=='O'), 'effectif'] = 1
    df.loc[(df.inspr=='O'), 'effs'] = 0
    df.loc[(df.inspr!='O'), 'effectif'] = 0
    df.loc[(df.inspr!='O'), 'effs'] = 1
    df.loc[:, 'efft'] = 1
 
    mask = (df.typ_dipl=='PJ')
    df['eff_dei'] = np.where(mask, 1, 0)
    for i in ['effectif', 'effs', 'efft', 'nbach']:
        df.loc[mask, i] = 0

    df = df.assign(effectif_cesure = np.where(df.amena=='3', df.effectif, 0))
    df = df.assign(nbach_cesure = np.where(df.amena=='3', df.nbach, 0))

    mask = ((df.curpar!='02')&(df.conv=='P'))|(df.curpar=='02')
    df.loc[mask, 'eff_ss_cpge']=0
    df.loc[mask, 'nbach_ss_cpge']=0
    df.loc[mask, 'efft_ss_cpge']=0
    df.loc[mask, 'effs_ss_cpge']=0

    mask = (df.conv!='P') & (df.curpar!='02')
    df.loc[mask, 'eff_ss_cpge']=df.loc[mask, 'effectif']
    df.loc[mask, 'nbach_ss_cpge']=df.loc[mask, 'nbach']
    df.loc[mask, 'efft_ss_cpge']=df.loc[mask, 'efft']
    df.loc[mask, 'effs_ss_cpge']=df.loc[mask, 'effs']

    return df