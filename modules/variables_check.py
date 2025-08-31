from reference_data.bcn import *
from reference_data.google_sheet import *
from reference_data.ref_data_utils import reference_data_loader
from config_path import PATH
import pandas as pd, json, numpy as np

global CORRECTIFS_dict, BCN, PAYSAGE_id
CORRECTIFS_dict = reference_data_loader('google_sheet')
BCN = reference_data_loader('bcn')


def vars_sise_to_be_check(year):

    CONF = json.load(open('utils/config_sise.json', 'r'))
    vars_sise = pd.read_pickle(f"{PATH}output/items_by_vars{year}.pkl", compression='gzip')
    hors_nomen=pd.DataFrame()

    for conf in CONF:
        if conf['var_sise'] not in ['etabli', 'compos', 'etabli_diffusion']:
            var_sise = conf["var_sise"]
            nomen = conf["n_data"]

            if nomen:
                if var_sise in ['cometa', 'comins']:
                    # l=pd.DataFrame.from_dict(CORRECTIFS_dict[nomen]).iloc[:,0].unique()
                    l=(pd.DataFrame.from_dict(CORRECTIFS_dict['LES_COMMUNES'])[
                                                ['COM_CODE', 'COM_NOM']]
                                                .rename(columns={'COM_CODE':'item', 'COM_NOM':'libelle'}))
                else:
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
                            
                tmp=vars_sise.loc[(vars_sise.variable==var_sise)].assign(nomenclature=nomen)
                tmp=tmp.assign(hors_nomenclature=np.where(~tmp.item.isin(l.item.unique()), '1', '0'))
                tmp.loc[tmp.item=='-1', 'hors_nomenclature'] = '0'
                tmp=tmp.merge(l, how='left', on='item')
                hors_nomen=pd.concat([hors_nomen, tmp], ignore_index=True)  
    
    hors_nomen = pd.concat([hors_nomen, vars_sise.loc[vars_sise.variable=='etabli_diffusion']])
    hors_nomen=hors_nomen[['rentree','source','variable','nomenclature','item','libelle','count','hors_nomenclature']]
			 
    with pd.ExcelWriter(f"{PATH}vars_review_{year}.xlsx", mode='w', engine="openpyxl") as writer:  
        hors_nomen.to_excel(writer, sheet_name='vars_hs_norme', index=False,  header=True, na_rep='', float_format='str')