from reference_data.bcn import *
from reference_data.google_sheet import *
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
from config_path import PATH
from utils.functions_shared import get_individual_source
import pandas as pd, json, numpy as np



def cursus_to_check(year, df):
    t1 = (df.loc[df.cursus_lmd.isin(['-1', 'X', '9'])]
            .drop(columns='effectif')
            .drop_duplicates())
    with pd.ExcelWriter(f"{PATH}vars_review_cleaned_{year}.xlsx", mode='a', if_sheet_exists="replace") as writer:  
        t1.to_excel(writer, sheet_name='cursus_lmd', index=False)


def vars_compare_with_bcn(year, df):
    t1 = (df.loc[(df.typ_dipl!=df.typ_dipl_orig)|(df.sectdis!=df.sectdis_orig)]
          .drop(columns='effectif')
          .drop_duplicates())
    t1 = t1.loc[~(t1.typ_dipl.isnull()&t1.sectdis.isnull())]

    with pd.ExcelWriter(f"{PATH}vars_review_cleaned_{year}.xlsx", mode='a', if_sheet_exists="replace") as writer:  
        t1.to_excel(writer, sheet_name='vars_difference', index=False)


def diplom_to_check_by_source(year, hors_nomen, df):
    # zip_path=os.path.join(PATH, f"output/sise_cleaned_{year}.zip")

    # diplom
    tmp = pd.DataFrame()
    dip = hors_nomen.loc[(hors_nomen.variable=='diplom')&(hors_nomen.hors_nomenclature=='1'), ['rentree', 'source', 'item']]

    # for rentree in dip.rentree.unique():
    # tmp = get_individual_source(zip_path, 'sise', rentree)
    df = df.merge(dip, how='inner', left_on=['rentree', 'source', 'diplom'], right_on=['rentree', 'source', 'item'])
    tmp = pd.concat([tmp, df[['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'compos', 'typ_dipl', 'typ_dipl_orig', 'diplom', 'sectdis', 'sectdis_orig', 'cursus_lmd']].drop_duplicates()], ignore_index=True)

    with pd.ExcelWriter(f"{PATH}vars_review_cleaned_{year}.xlsx", mode='a', if_sheet_exists="replace") as writer:  
        tmp.to_excel(writer, sheet_name='diplom', index=False)


def vars_sise_to_be_check(year, stage, df):

    CONF = json.load(open('utils/config_sise.json', 'r'))
    vars_sise = pd.read_pickle(f"{PATH}output/items_{stage}_by_vars{year}.pkl", compression='gzip')
    hors_nomen=pd.DataFrame()

    for conf in CONF:
        var_sise = conf["var_sise"]
        nomen = conf["n_data"]
        # print(f"{var_sise} -> {nomen}")

        if var_sise in ['etabli', 'compos', 'etabli_diffusion']:
            continue

        if not nomen:
            continue

        if nomen in BCN:
            if var_sise in BCN[nomen].columns:
                if 'libelle_long' in BCN[nomen].columns:
                    l=BCN[nomen][[var_sise, 'libelle_long']].rename(columns={var_sise:'item', 'libelle_long':'libelle'})
                else:
                    print(f"- trouver le libelle pour {var_sise}\n{BCN[nomen].columns}")
            else:
                # print(f"*> {var_sise} n'existe pas dans {nomen}\n  - le code suivant va extraire la 1ere colonne {BCN[nomen].columns[0]}")
                if 'libelle_long' in BCN[nomen].columns:
                    l=BCN[nomen][[BCN[nomen].columns[0], 'libelle_long']].rename(columns={BCN[nomen].columns[0]:'item', 'libelle_long':'libelle'})
                else:
                    libelle_columns = [col for col in BCN[nomen].columns if col.startswith('libelle')]
                    l=BCN[nomen][[BCN[nomen].columns[0], libelle_columns[0]]].rename(columns={BCN[nomen].columns[0]:'item', libelle_columns[0]:'libelle'})
        
        if nomen in CORRECTIFS_dict:
            if var_sise not in ['cometa', 'comins']:
                l = pd.DataFrame.from_dict(CORRECTIFS_dict[nomen]).iloc[:, :2]
            l.columns = ['item', 'libelle']
            
        tmp=vars_sise.loc[(vars_sise.variable==var_sise)].assign(nomenclature=nomen)
        tmp=tmp.assign(hors_nomenclature=np.where(~tmp.item.isin(l.item.unique()), '1', '0'))
        tmp.loc[tmp.item=='-1', 'hors_nomenclature'] = '0'
        tmp=tmp.merge(l, how='left', on='item')
        hors_nomen=pd.concat([hors_nomen, tmp], ignore_index=True)  

    # hors_nomen = pd.concat([hors_nomen, vars_sise.loc[vars_sise.variable=='etabli_diffusion']])
    hors_nomen=hors_nomen[['rentree','source','variable','nomenclature','item','libelle','count','hors_nomenclature']]
    with pd.ExcelWriter(f"{PATH}vars_review_{stage}_{year}.xlsx", mode='w', engine="openpyxl") as writer:  
        hors_nomen.to_excel(writer, sheet_name='vars_hs_norme', index=False,  header=True, na_rep='', float_format='str')

    if stage=='cleaned':
        diplom_to_check_by_source(year, hors_nomen, df)
        vars_compare_with_bcn(year, df)
        cursus_to_check(year, df)



def commune_checking(year, df):
    l = pd.DataFrame.from_dict(CORRECTIFS_dict['LES_COMMUNES'])[['com_code', 'com_nom']]
    for i in ['cometa', 'comins']:

        if i=='cometa':
            cols = list(set(df.columns).difference(set(['compos', 'comins'])))
            tmp = df[cols].drop_duplicates()
        else:
            tmp = df.copy()

        tmp = (tmp.merge(l, how='left', left_on=i, right_on='com_code')
                .rename(columns={'com_nom':f'{i}_name'})
                .drop(columns='com_code')
            )


        if i == 'comins':
            tmp = tmp.assign(hors_nomenclature=np.where(tmp[f"{i}_name"].isnull(), '1', '0')).sort_values(['rentree', 'id_paysage', 'compos'])
        else:
            tmp = tmp.assign(hors_nomenclature=np.where(tmp[f"{i}_name"].isnull(), '1', '0')).sort_values(['rentree', 'id_paysage'])

        tmp['nb']=tmp.groupby(list(set(tmp.columns).difference(set([i]))), dropna=False).transform('size')

        if os.path.isfile(os.path.join(PATH, 'vars_communes.xlsx')):
            with pd.ExcelWriter(f"{PATH}vars_communes.xlsx", mode='a', engine="openpyxl", if_sheet_exists="replace") as writer:  
                tmp.to_excel(writer, sheet_name=i, index=False,  header=True, na_rep='')
        else:
            with pd.ExcelWriter(f"{PATH}vars_communes.xlsx", mode='w', engine="openpyxl") as writer:  
                tmp.to_excel(writer, sheet_name=i, index=False,  header=True, na_rep='')
    

def etab_checking(year, df):
    
    # saclay
    df_saclay = df.loc[(df.etabli=='0912408Y')&(df.inspr=='O'), ['rentree', 'source', 'compos', 'typ_dipl', 'diplom', 'effectif']]
    df_saclay.to_pickle(f"{PATH}output/saclay_data_{year}.pkl", compression='gzip')

    # effectif inscr by all uai/paysage
    x = df.loc[df.inspr=='O', ['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'rattach', 'compos', 'effectif']]
    x = (x.groupby(list(set(x.columns).difference(set(['effectif']))), dropna=False)
          .agg({'effectif': 'sum'})
          .reset_index()
        )
    x.to_pickle(f"{PATH}output/effectif_by_etab_{year}.pkl", compression='gzip')

    # effectif by dip ing
    dd=['AC', 'CD', 'CV', 'CZ', 'DL', 'DP', 'FH', 'FI', 'FJ', 'FN', 'IB', 
        'ID', 'JC', 'JD', 'JF', 'MA', 'MB', 'PB', 'PC', 'PL', 'RA', 'RB', 
        'RC', 'RD', 'RG', 'RH', 'UH', 'UJ', 'UK', 'XA', 'XB', 'XD', 'YB', 
        'YI', 'DR']

    y1 = df.loc[(df.inspr=='O')&(df.typ_dipl.isin(dd)), 
                ['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'rattach', 'compos', 'typ_dipl', 'typ_dipl_orig', 'diplom', 'effectif']]
    y1 = (y1.groupby(list(set(y1.columns).difference(set(['effectif']))), dropna=False)
            .agg({'effectif': 'sum'})
            .reset_index()
            )

    y2 = df.loc[(df.inspr=='O')&(~df.typ_dipl.isin(dd)), 
                ['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'rattach', 'compos', 'typ_dipl', 'typ_dipl_orig', 'effectif']]
    y2 = (y2.groupby(list(set(y2.columns).difference(set(['effectif']))), dropna=False)
            .agg({'effectif': 'sum'})
            .reset_index()
            )
    y = pd.concat([y1, y2], ignore_index=True)
    y.to_pickle(f"{PATH}output/effectif_by_etab_diplom_{year}.pkl", compression='gzip')

    # uai_fresq
    x = df.loc[df.inspr=='O', ['rentree', 'source', 'etabli', 'id_paysage', 'lib_paysage', 'rattach', 'uai_fresq', 'inf', 'effectif']]
    x = (x.groupby(list(set(x.columns).difference(set(['effectif']))), dropna=False)
          .agg({'effectif': 'sum'})
          .reset_index()
        )
    x.to_pickle(f"{PATH}output/effectif_for fresq_{year}.pkl", compression='gzip')