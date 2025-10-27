from config_path import PATH
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
from utils.functions_shared import replace_by_nan
import json, pandas as pd, numpy as np



def delete(df):
    import pandas as pd

    try:
        print(f"- remove FLAG_SUP=1")
        df = df.loc[df.flag_sup!='1']
    except:
        print("- no flag_sup in df")

    x=pd.DataFrame(CORRECTIFS_dict['delete'])
    mask = (x.etabli_ori_uai!='')
    x.loc[mask, 'etabli'] = x.loc[mask, 'etabli_ori_uai']
    x=x.drop(columns='etabli_ori_uai')

    delete_dict = [
        {
            col: int(val) if col == "rentree" else str(val) if col == "diplom" else val 
            for col, val in ligne.items() 
            if val != ""
        }
        for ligne in x.to_dict(orient='records')
        ]

    try:
        print(f"- remove rows to delete")
        for item in delete_dict:
            item_filtered = {k: v for k, v in item.items() if k in df.columns}
            # On convertit les valeurs de 'rentree' et 'diplom' pour qu'elles correspondent au type du DataFrame
            mask = (df[list(item_filtered.keys())] == pd.Series(item_filtered)).all(axis=1)
            df = df[~mask]
        print(f"- size after delete rows: {len(df)} ")
    except:
        print("- no remove rows in df")
    return df


def vars_no_empty(df):
    import json, pandas as pd
    CONF=json.load(open('utils/config_sise.json', 'r'))
    no_empty = [i['var_sise'] for i in CONF if i['empty']=='NO']

    for c in no_empty:
        print(f"- {c} -> {df[c].unique()}")

    print("- EFFECTIF clean")
    try:
        if any(~df.effectif.isin([0,1])):
            print("# ATTENTION ! EFFECTIF not valid item")
    except KeyError:
        print("# ATTENTION ! EFFECTIF not in df")    


    print("- AGE clean")
    if any(df.age.isnull()):
        print("- ATTENTION age is null")
        x = (df.loc[(df.age.isnull())&(df.inspr=='O'), ['rentree', 'source', 'etabli', 'id_paysage', 'effectif']]
                .groupby(['rentree', 'source', 'etabli', 'id_paysage'], dropna=False)
                .agg({'effectif': 'sum'})
                .reset_index()
            )
        with pd.ExcelWriter(f'{PATH}output/age_tobe_checked.xlsx') as writer:
            x.to_excel(writer, sheet_name=f'{str(df.rentree.unique()[0])}', index=False)

    else:
        try:
            liste_entiers = [x.astype('int64') for x in df.age.unique()]
            liste_triee = sorted(liste_entiers)
            # Vérifications
            valeur_min = min(liste_triee)
            valeur_max = max(liste_triee)

            if valeur_min<14 or valeur_max>99:
                print(f"### ATTENTION ! age min < {valeur_min}, age max > {valeur_max}")
        # Tri de la liste
        except ValueError:
                print(f"### {[str(x).isdigit() and x != 0 for x in liste_entiers]}")


#####################################################
## ETABLISSEMENT ####################################
def comins_clean(df):
    try:
        print(f"- COMINS clean")
        comins_dict=json.load(open('patches/comins.json', 'r'))
        def get_first_value(comins_dict, key):
            if key in comins_dict:
                inner_dict = comins_dict[key]
                # Retourne la première valeur du dictionnaire interne
                return next(iter(inner_dict.values()))
            return None

        # Mise à jour de la colonne 'comins'
        for i, row in df.iterrows():
            key = row['ui']
            if key in comins_dict:
                df.at[i, 'comins'] = get_first_value(comins_dict, key)

    except:
        print("- COMINS not clean")

    return df.rename(columns={'comins':'comui'})

def cometa_clean(df):
    try:
        print(f"- COMETA clean")
        x=pd.DataFrame(CORRECTIFS_dict['A_COMETA']) 
        x['rentree'] = x.rentree.astype(int)
        df = df.rename(columns={'cometa':'cometa_orig'}).merge(x, how='left', on=['rentree', 'id_paysage'])
        df.loc[df.cometa.isnull(), 'cometa'] = df.loc[df.cometa.isnull()].cometa_orig
    except:
        print("- COMETA not clean")
    return df


####################################################
## COMPTAGE ########################################
# INSPR
def inspr_clean(df): 
    try:
        print("- INSPR clean")
        df.loc[df.inspr=='0', 'inspr'] = 'O'
    except:
        print("# ATTENTION - INSPR not in df")
    return df

def effectif_clean(df):
    try:
        print("- EFFECTIF clean")
        if any(~df.effectif.isin([0,1])):
            print("# ATTENTION ! EFFECTIF not valid item")
    except KeyError:
        print("# ATTENTION ! EFFECTIF not in df")   


######################################################
## BAC ###############################################
# BAC
def bac_clean(df):

    def bac_a(v):
        if (v.startswith('A6') or
            v.startswith('A7') or
            (v.startswith('A') and '-' in v)):
            # On prend les deux premières lettres et on ajoute '-85'
            return v[:2] + '-85'
        
        if v.isdigit() and len(v) == 2:
            v = v.zfill(4)
        else:
            return v

    try:
        print(f"- BAC clean")
        c = list(BCN['N_BAC'].bac)

        df['bac'] = df['bac'].apply(bac_a)
        df.loc[~df.bac.isin(c), 'bac'] = '999'
    except:
        print("- BAC not clean")   
    return df

def bac_regroup_clean(df):
    try:
        print(f"- BAC_RGRP clean")
        bd = BCN['N_BAC'][['bac', 'bac_regroupe_2']].drop_duplicates()
        df = df.merge(bd, how='left', on='bac').rename(columns={'bac_rgrp':'bac_rgrp_orig', 'bac_regroupe_2':'bac_rgrp'})

        df['bac_rgrp'] = replace_by_nan(df.bac_rgrp)
        df.loc[df.bac_rgrp.isnull(), 'bac_rgrp'] = '9'    
        df.loc[(df.bac_rgrp=='9')&(df.anbac==-1), 'bac_rgrp'] = '7'
        df.loc[df.bac_rgrp.isin(["1","2","3"]), 'bac_rgrp'] = 'A'

    except:
        print("- BAC_RGRP not clean")   
    return df

def bac_loc_clean(df):
    dep_dict = {
    item['depbac_deprespa_in']: item['depbac_deprespa_out']
    for item in CORRECTIFS_dict['DEP_ACA_CORRECTIF']
        }
    
    try:
        print("- DEP clean")
        for i in ['deprespa', 'depbac']:
            df[i] = df[i].replace(dep_dict)
            df.loc[df[i]=="-1", i] = '999'
        
        # si depbac = etranger mais acabac est localisé en FR -> redressement de depbac en FR non idenrifié
        df.loc[(df.depbac=='999')&(~df.acabac.isin(['00', '-1'])), 'depbac'] = '000'
    except:
        print("- DEP not clean")
    
    aca_dict = {
    item['depbac_deprespa_out']: item['acarespa_acabac']
    for item in CORRECTIFS_dict['DEP_ACA_CORRECTIF']
        }

    print("-- fix depbac with acabac")
    aca_df = pd.DataFrame(CORRECTIFS_dict['LES_COMMUNES'])[['dep_code', 'aca_code']].drop_duplicates()
    # conserver les acabac qui ne contiennent qu'un seul département et remplacer depbac='000' par ce dep_code
    aca_df = aca_df.groupby(['aca_code']).filter(lambda x: x['dep_code'].count() == 1.)
    aca_df['dep_code'] = aca_df['dep_code'].str.zfill(3)
    df = df.merge(aca_df, how='left', left_on='acabac', right_on='aca_code')
    df.loc[(df.depbac=='000')&(~df.dep_code.isnull()), 'depbac'] = df.loc[(df.depbac=='000')&(~df.dep_code.isnull()), 'dep_code']
    df = df.drop(columns=['dep_code', 'aca_code'])

    try:
        print("- ACA clean")
        df['ac'] = df['depbac'].replace(aca_dict)
        # si depbac FR non identifié ('000') mais acabac connu conserver acabac
        df.loc[(~df.acabac.isin(['-1', '00']))&(df.ac!=df.acabac)&(df.ac=='99')&(df.depbac=='000'), 'ac'] = df.loc[(df.acabac!='-1')&(df.ac!=df.acabac)&(df.ac=='99')&(df.depbac=='000'), 'acabac'] 
        
        df = df.drop(columns='acabac').rename(columns={'ac':'acabac'})
    except:
        print("- ACA not clean")
    
    return df


#####################################################
## FORMATION ########################################

def situpre_clean(df):
    try:
        print("- SITUPRE clean")   
        c = list(BCN['N_SITUATION_ANNEE_PRECEDENTE'].situation_annee_precedente)
        df.loc[~df.situpre.isin(c), 'situpre'] = '9'
    except:
        print("- SITUPRE not clean")   
    return df

# NUMED
def ed_clean(df):
    try:
        print(f"- ED clean")
        df['numed'] = df['numed'].str.lstrip('0')
    except KeyError:
        print("# ATTENTION ! numed not in df")   
    return df

# DIPLOM
def diplom_empty(df):
    try:
        print(f"- DIPLOM EMPTY clean")
        diplom_dict=json.load(open('patches/diplom_empty.json', 'r'))
        for etabli, diplom in diplom_dict.items():
            df.loc[(df.etabli==etabli)&(df.diplom=='-1'), 'diplom'] = diplom
        
        df.loc[df.diplom=='-1', 'diplom'] = '9999999'
    except (AttributeError, KeyError, ValueError) as e:
        print(f"Une erreur est survenue : {type(e).__name__} - {e}")
    return df

def diplom_to_vars_bcn(df):
    print(f"-- add TYP_DIPL/SECTDIS/NATURE by DIPLOM from BCN")
    b = (BCN['N_DIPLOME_SISE'][['diplome_sise', 'type_diplome_sise', 'secteur_disciplinaire_sise']]
         .rename(columns={
                'diplome_sise':'diplom',
                'type_diplome_sise':'typ_dipl',
                'secteur_disciplinaire_sise':'sectdis'
         }))
    df = (df.rename(columns={
                'typ_dipl':'typ_dipl_orig',
                'sectdis':'sectdis_orig'
                })
            .merge(b, how='left', on='diplom'))
    
    for c in ['typ_dipl', 'sectdis']:
        corig = f"{c}_orig"
        df.loc[df[c].isnull(), c] = df.loc[df[c].isnull(), corig] 

    # add NATURE
    b = (BCN['N_TYPE_DIPLOME_SISE'][['type_diplome_sise', 'nature_diplome_sise']]
         .rename(columns={
                'type_diplome_sise':'typ_dipl',
                'nature_diplome_sise':'nature'
         })) 
    df = (df.rename(columns={'nature':'nature_orig'})
            .merge(b, how='left', on='typ_dipl'))
    df.loc[df.nature.isnull(), 'nature'] = df.loc[df.nature.isnull()].nature_orig

    return df

def sectdis_clean(df):
    dd = (pd.DataFrame(CORRECTIFS_dict['SECTDIS'])[['sectdis', 'discipline']]
             .rename(columns={'discipline':'discipli'}))
    
    print(f"- SECTDIS clean")
    df.loc[df.sectdis.isin(['44','45','46','47','48']), 'sectdis']="39"
    sl = list(dd.sectdis)
    df.loc[~df.sectdis.isin(sl), 'sectdis'] = '99'

    print("- DISCIPLI add")
    df = df.merge(dd, how='left', on='sectdis')   
    return df

def groupe_clean(df):
    try:
        print(f"- GROUPE clean")
        df.loc[(df.discipli=='11')&(df.groupe=='5'), 'groupe'] = 'A'
        grp_dict = {
        item['value1_in']: item['value_out']
        for item in CORRECTIFS_dict['GROUPE_CORRECTIF']
            }
        df['groupe'] = df['discipli'].map(grp_dict)
    except:
        print(f"- GROUPE not clean")
    return df

def niveau_clean(df):
    try:
        print(f"- NIVEAU clean")
        b = list(BCN['N_ANNEE_DANS_FORMATION_SISE'].annee_formation)
        df.loc[~df.niveau.isin(b), 'niveau'] = '01'
    except:
        print(f"- NIVEAU not clean")
    return df

def cursus_clean(df):
    from collections import defaultdict
    try:
        print(f"- CURSUS clean")
        groupes = defaultdict(list)

        lmd_dict =[{k: v for k, v in d.items() if v != ''} for d in CORRECTIFS_dict['CURSUS_LMD_CORRECTIF']]
        for d in lmd_dict:
            cles = tuple(sorted(k for k in d.keys() if k != 'cursus_lmd_out'))
            groupes[cles].append(d)

        # Pour chaque groupe de dictionnaires partageant les mêmes clés
        for cles, sous_liste in groupes.items():
            # Créer un DataFrame temporaire pour la sous-liste
            df_temp = pd.DataFrame(sous_liste)

            # Faire la jointure sur les clés communes
            df_merge = df.merge(
                df_temp,
                on=list(cles),
                how='left'
            )

            # Mettre à jour cursus_lmd avec cursus_lmd_out si correspondance
            mask = df_merge['cursus_lmd_out'].notna()
            df.loc[mask, 'cursus_lmd'] = df_merge.loc[mask, 'cursus_lmd_out']

        # 2d fix cursus_lmd
        lmd_dict ={item['typ_dipl']: item['cursus_lmd_out'] for item in CORRECTIFS_dict['CORRLMD']}
        df.loc[df.cursus_lmd.isin(['-1', 'X', '9']), 'cursus_lmd'] = df.loc[df.cursus_lmd.isin(['-1', 'X', '9']), 'typ_dipl'].map(lmd_dict).fillna('-1')

    except:
        print('- cursus_lmd no clean')
    return df

# CURPAR
def curpar_clean(df):
    try:
        print(f"- CURPAR clean")
        c = list(BCN['N_AUTRE_CURSUS_SISE'].autre_cursus_sise)
        df.loc[~df.curpar.isin(c), 'curpar'] = '99'
    except KeyError:
        print("# ATTENTION ! CURPAR not in df")       
    return df


def amena_clean(df):
    try:
        print(f"- AMENA clean")
        c = list(BCN['N_AMENA'].amena)
        df.loc[~df.amena.isin(c), 'amena'] = '9'
    except:
        print("- AMENA not clean")  
    return df

def conv_clean(df):
    try:
        print("- CONV clean")
        c = list(BCN['N_CONVENTION'].convention)
        df.loc[~df.conv.isin(c), 'conv'] = '99'
    except:
        print("- CONV not clean") 
    return df

def degetu_clean(df):
    try:
        print("- DEGETU clean")
        c = list(BCN['N_NIVEAU_FORMATION'].niveau_formation)
        df.loc[~df.degetu.isin(c), 'degetu'] = '9'
    except:
        print("- DEGETU not clean") 
    return df


def exoins_clean(df):
    try:
        print("- EXOINS clean")
        c = list(BCN['N_EXONERATIONS'].exoins)
        df.loc[~df.exoins.isin(c), 'exoins'] = '99'
    except:
        print("- EXOINS not clean")   
    return df

def echang_clean(df):
    try:
        print("- ECHANG clean")
        ec = list(BCN['N_PROGRAMME_ECHANGE_INTERNATIO'].programme_echange_internationa)
        df.loc[~df.echang.isnin(ec), 'echang'] == '0'
    except:
        print("- ECHANG not clean")   
    return df

#####################################################
### ETUDIANT ########################################
def country_clean(df):
    # NATION/PARIPA
    cd = pd.DataFrame(CORRECTIFS_dict['G_PAYS'])
    cd.mask(cd=='', inplace=True)
    c = list(cd.loc[cd.lib.isnull()].pays)

    for i in ['nation', 'paripa']:
        print(f"- {i.upper()} clean")
        df.loc[df[i].isin(c), i] = '999'
        df.loc[~df[i].isin(list(cd.pays)), i] = '999'
    return df


def pcs_clean(df):
    print("- PCS clean")
    c = list(BCN['N_PCS'].pcs)
    for i in ['pcspar', 'pcspar2']:
        try:
            df.loc[~df[i].isin(c), i] = '99'
        except KeyError:
            print(f"- {i.upper()} not in df")
    return df
