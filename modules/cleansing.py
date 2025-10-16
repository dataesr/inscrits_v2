from config_path import PATH
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
import json, pandas as pd



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
            key = row['compos']
            if key in comins_dict:
                df.at[i, 'comins'] = get_first_value(comins_dict, key)

    except:
        print("")

    return df



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




## FORMATION ########################################
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
    print(f"-- add TYP_DIPL/SECTDIS by DIPLOM from BCN")
    b = (BCN['N_DIPLOME_SISE'][['diplome_sise', 'type_diplome_sise', 'secteur_disciplinaire_sise']]
         .rename(columns={
                'diplome_sise':'diplom',
                'type_diplome_sise':'typ_dipl',
                'secteur_disciplinaire_sise':'sectdis'
         })
        )
    
    df = (df.rename(columns={
                'typ_dipl':'typ_dipl_orig',
                'sectdis':'sectdis_orig'
                })
            .merge(b, how='left', on='diplom')
        )
    
    for c in ['typ_dipl', 'sectdis']:
        corig = f"{c}_orig"
        df.loc[df[c].isnull(), c] = df.loc[df[c].isnull(), corig] 

    return df

def sectdis_clean(df):
    print(f"- SECTDIS clean")
    df.loc[df.sectdis.isin(['44','45','46','47','48']), 'sectdis']="39"
    return df

def niveau_clean(df):
    print(f"- NIVEAU clean")
    b = list(BCN['N_NIVEAU_SISE'].niveau_sise)
    df.loc[~df.niveau.isin(b), 'niveau'] = '01'
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
        df.loc[df.curpar.isin(['-1', 'N']), 'curpar'] = '00'
    except KeyError:
        print("# ATTENTION ! CURPAR not in df")       
    return df

# FRESQ
def enrich_fresq(df):
    print(f"- FRESQ enrich")
    uai_fresq = pd.DataFrame(CORRECTIFS_dict['O_INF_FRESQ']).assign(rentree=lambda x: x['rentree'].astype(int))
    df = df.merge(uai_fresq, how='left', on=['rentree', 'rattach', 'diplom'])
    return df



### ETUDIANT ########################################
def nation_clean(df):
    # NATION
    print(f"- NATION clean")
    df.loc[df.nation.isin(['$']), 'nation'] = '-1'
    # cp = json.load(open('patches/country.json', 'r'))
    # df['nation'] = df['nation'].replace(cp)
    c = pd.DataFrame(CORRECTIFS_dict['G_PAYS'])
    c.mask(c=='', inplace=True)
    c = c.loc[c.lib.isnull()].pays.to_list()
    df.loc[df.nation.isin(c), 'nation'] = '999'
    return df




### COMPLETE ####################################################################################################
def data_cleansing(df, etab, meef):
    import pandas as pd, numpy as np

    # df = delete(df)

    # ETABLI & COMPOS & RATTACH
    df = df.drop(columns='rattach')
    df = df.rename(columns={'etabli':'etabli_old', 'compos':'compos_old'})
    df = (df.merge(etab[['rentree', 'etabli_old', 'etabli', 'compos_old', 'compos', 'rattach', 'id_paysage', 'lib_paysage']], 
                how='left', on=['rentree', 'etabli_old', 'compos_old'])
        )
    df = df.loc[:, ~df.columns.str.contains('_old')]

    # remove etabli without id_paysage
    df.loc[df.id_paysage=='', 'id_paysage'] = np.nan
    if len(df.loc[df.id_paysage.isnull()])>0:
        x=df.loc[df.id_paysage.isnull(), ['rentree', 'source', 'etabli']].drop_duplicates()
        print(f"- etabli without id_paysage by source: {x.source.value_counts()}")
        df = df.loc[~((df.source.isin(['mana', 'culture', 'enq26bis']))&(df.id_paysage.isnull()))]
    
    print(f"- size after remove etabli without paysage: {len(df)}")

    # ETABLI_DIFFUSION
    try:
        df = df.merge(meef, how='left').drop(columns='etabli_diffusion').rename(columns={'new_lib':'etabli_diffusion'})
    except KeyError:
        return print(f'no etabli_diffusion into sise {df.rentree.unique()}')
    
    ##########################
    # df = df.mask(df=='')

    effectif_clean(df)

    df = inspr_clean(df)
    df = diplom_empty(df)
    df = diplom_to_vars_bcn(df)
    df = sectdis_clean(df)
    df = niveau_clean(df)
    df = cursus_clean(df)
    df = curpar_clean(df)
    df = enrich_fresq(df)


    df = ed_clean(df)

    

    df = nation_clean(df)


    df = comins_clean(df)


    
    return df

