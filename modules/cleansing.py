from config_path import PATH
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
import json, pandas as pd

def delete(df):
    import pandas as pd
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

    for item in delete_dict:
        item_filtered = {k: v for k, v in item.items() if k in df.columns}
        # On convertit les valeurs de 'rentree' et 'diplom' pour qu'elles correspondent au type du DataFrame
        mask = (df[list(item_filtered.keys())] == pd.Series(item_filtered)).all(axis=1)
        df = df[~mask]

    print(f"- size after delete rows: {len(df)} ")
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

    return df

## ETABLISSEMENT ####################################
# NUMED
def ed_clean(df):
    print(f"- ED clean")
    try:
        df['numed'] = df['numed'].str.lstrip('0')
    except KeyError:
        print("# ATTENTION ! numed not in df")   
    return df

## COMPTAGE ########################################
# INSPR
def inspr_clean(df):
    print("- INSPR clean")
    try:
        df.loc[df.inspr=='0', 'inspr'] = 'O'
    except:
        print("# ATTENTION - INSPR not in df")
    return df

def effectif_clean(df):
    print("- EFFECTIF clean")
    try:
        if any(~df.effectif.isin([0,1])):
            print("# ATTENTION ! EFFECTIF not valid item")
    except KeyError:
        print("# ATTENTION ! EFFECTIF not in df")   




## FORMATION ########################################
# DIPLOM
def diplom_empty(df):
    print(f"- DIPLOM EMPTY clean")
    try:
        diplom_dict=json.load(open('patches/diplom_empty.json', 'r'))
        for etabli, diplom in diplom_dict.items():
            df.loc[(df.etabli==etabli)&(df.diplom=='-1'), 'diplom'] = diplom
        
        df.loc[df.diplom=='-1', 'diplom'] = '9999999'
    except (AttributeError, KeyError, ValueError) as e:
        print(f"Une erreur est survenue : {type(e).__name__} - {e}")
    return df

# CURPAR
def curpar_clean(df):
    print(f"- CURPAR clean")
    try:
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

    df = delete(df)

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

    df = enrich_fresq(df)

    # df = vars_no_empty(df)

    df = ed_clean(df)

    df = curpar_clean(df)

    df = nation_clean(df)





    
    return df

