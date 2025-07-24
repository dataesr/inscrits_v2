from api_process.paysage import *
from nomenclatures.bcn import *
from nomenclatures.google_sheet import *
from config_path import PATH
import pandas as pd, json, numpy as np

global CORRECTIFS_dict, BCN, PAYSAGE_id
CORRECTIFS_dict = get_all_correctifs()
BCN = get_all_bcn()
PAYSAGE_id = get_paysage_id()


def etabli_paysage(year):
    tmp=pd.read_pickle(f"{PATH}output/frequency_etabli_source_year{year}.pkl",compression= 'gzip')
    print(f"- size etabli {len(tmp)}")

    tmp=tmp.merge(pd.DataFrame(PAYSAGE_id)[['id_value','id_paysage','active','id_enddate']], how='left', left_on='etabli', right_on='id_value').drop_duplicates()

    tmp=tmp.assign(paysage_presence=np.where(tmp.id_value.isnull(), 'N', 'Y'))
    tmp.loc[~tmp.id_enddate.isnull(), 'end_year'] = tmp.loc[~tmp.id_enddate.isnull()].id_enddate.str.split('-').str[0]
    tmp['pid_multi']=tmp.groupby(['rentree', 'source', 'etabli', 'compos']).transform('size')

    print(f"- size etabli+paysage {len(tmp)}")
    return tmp


def vars_sise_to_be_check(year):
    from step2_first_control.variables_check import etabli_paysage

    CONF=json.load(open('utils/config_sise.json', 'r'))
    vars_sise = pd.read_pickle(f"{PATH}output/items_by_vars{year}.pkl", compression='gzip')

    hors_nomen=pd.DataFrame()

    for conf in CONF:
        var_sise = conf["var_sise"]
        nomen = conf["n_data"]

        if nomen:
            if var_sise in ['cometa', 'comins']:
                l=pd.DataFrame.from_dict(CORRECTIFS_dict[nomen]).iloc[:,0].unique()
            elif var_sise in ['etabli']:
                etabli=etabli_paysage(year)
            else:
                if var_sise in BCN[nomen].columns:
                    l=BCN[nomen][var_sise].unique()
                else:
                    print(f"*> le nom de variable {var_sise} n'existe pas dans {nomen}\n - le code suivant va extraire la 1ere colonne {BCN[nomen].columns[0]}")
                    l=BCN[nomen].iloc[:,0].unique()

            tmp=vars_sise.loc[(vars_sise.variable==var_sise)].assign(nomenclature=nomen)
            tmp.loc[~tmp.item.isin(l), 'hors_nomenclature'] = '1'
            hors_nomen=pd.concat([hors_nomen, tmp], ignore_index=True)  

    with pd.ExcelWriter(f"{PATH}vars_review_{year}.xlsx", mode='w', engine="openpyxl") as writer:  
        hors_nomen.to_excel(writer, sheet_name='vars_hs_norme', index=False,  header=True, na_rep='', float_format='str')
        etabli.to_excel(writer, sheet_name='etabli_paysage', index=False,  header=True, na_rep='')