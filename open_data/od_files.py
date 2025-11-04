from config_path import PATH
from utils.functions_shared import *

def od_create_files(df):

    # sas opendata19 lignes 235 -> 279
    va=cols_selected['od_vars_odod']
    vn=cols_selected['od_vars_num']

    for i in va:
        df[i] = replace_by_nan(df[i])

    od = df.groupby(va, dropna=False)[vn].sum().reset_index()

    # sas opendata19 lignes 275 (data txt) -> odod.txt
    (rename_variables(od, 'names_vars_num')
     .loc[(od.rentree > 2014)&(od.operateur_lolf_150=='O')].to_csv(f"{PATH}opendata/odod.txt", na_rep='', encoding='utf-8', index=False, sep='\t')
    )
    ###########################
    # sas opendata19 lignes 289 -> 319
    # data for the next OD script
    va=cols_selected['od2_vars']
    vn=cols_selected['od_vars_num_init']
    od2p = df.groupby(va, dropna=False)[vn].sum().reset_index()

    return od, od2p