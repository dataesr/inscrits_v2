def vars_sise_to_be_check(year, bcn, com):
    from config_path import PATH
    from utils.vars_in_nomen import vbcn
    import pandas as pd, numpy as np

    vars_sise = pd.read_pickle(f"{PATH}output/items_by_vars{year}.pkl", compression='gzip')

    hors_nomen=pd.DataFrame()

    for k,v in vbcn.items():
        if k in ['cometa', 'comins']:
            l=pd.DataFrame.from_dict(com[v]).iloc[:,0].unique()
        else:
            if k in bcn[v].columns:
                l=bcn[v][k].unique()
            else:
                print(f"- le nom de variable {k} n'existe pas dans {v}\n - le code suivant va extraire la 1ere colonne {bcn[v].columns[0]}")
                l=bcn[v].iloc[:,0].unique()
            
        tmp=vars_sise.loc[(vars_sise.variable==k)].assign(nomenclature=v)
        tmp.loc[~tmp.item.isin(l), 'hors_nomenclature'] = '1'
        hors_nomen=pd.concat([hors_nomen, tmp], ignore_index=True)  

    hors_nomen.to_csv(f"{PATH}test/vars_hs_nomen.csv", sep=';', encoding='utf-8', index=False, na_rep='', float_format='str')
   
    return hors_nomen