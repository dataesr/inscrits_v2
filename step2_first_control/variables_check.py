def vars_sise_to_be_check(year, bcn):
    from config_path import PATH
    from utils.bcn_vars import vbcn
    import pandas as pd


    vars_sise = pd.read_pickle(f"{PATH}output/items_by_vars{year}.pkl", compression='gzip')

    for k in ['oppos', 'flag_meef', 'flag_sup']:
        if k=='oppos':
            vars_sise.loc[(vars_sise.variable==k)&(~vars_sise.item.isin(['O', 'N'])), 'hors_nomenclature'] = '1'
        else: 
            vars_sise.loc[(vars_sise.variable==k)&(~vars_sise.item.isin(['0', '1'])), 'hors_nomenclature'] = '1'

    for k,v in vbcn.items():
        bcn[v].columns=bcn[v].columns.str.lower()
        if k in bcn[v].columns:
            l=bcn[v][k].unique()
        else:
            print(f"- le nom de variable {k} n'existe pas dans {v}\n - le code suivant va extraire la 1ere colonne {bcn[v].columns[0]}")
            l=bcn[v].iloc[:,0].unique()
            
        vars_sise.loc[(vars_sise.variable==k)&(~vars_sise.item.isin(l)), 'hors_nomenclature'] = '1'    

    vars_sise.loc[vars_sise.item.isnull(), 'item']='NULL'
    vars_sise.to_csv(f"{PATH}test/vars_hs_nomen.csv", sep=';', encoding='utf-8', index=False, na_rep='')
   
    return vars_sise