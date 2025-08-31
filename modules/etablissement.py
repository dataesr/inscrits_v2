from reference_data.ref_data_utils import reference_data_loader
from config_path import PATH
import pandas as pd, json, numpy as np, pyreadstat

global CORRECTIFS_dict, BCN, PAYSAGE_id
CORRECTIFS_dict = reference_data_loader('google_sheet')
BCN = reference_data_loader('bcn')
PAYSAGE_id = reference_data_loader('paysage_id')


def etabli_meef(year):
    from reference_data.ref_data_utils import lookup_table
    meef = pd.read_pickle(f"{PATH}output/meef_frequency_source_year{year}.pkl",compression= 'gzip')

    # create new_lib with correction etabli_diffusion normandie nouvelle caledonie
    mapping_fix = json.load(open("patches/etabli_meef.json"))
    def fix(row):
        etabli = row['etabli']
        flag = row['flag_meef']
        if pd.notna(etabli) and etabli in mapping_fix and flag=='1':
            return mapping_fix[etabli.upper()]
        else:
            return row['etabli_diffusion']
        
    meef['new_lib'] = meef.apply(fix, axis=1)
  
    # keep only inspe espe and add id_paysage (id_paysage_formens)
    meef = meef.loc[(meef.flag_meef=='1')&(meef.new_lib.str.lower().str.startswith(('inspe', 'espe'), na=False))]
    meef_paysage = CORRECTIFS_dict['ETABLI_DIFFUSION_ID']
    mapping = {
    item['IN']: item['OUT']
    for item in meef_paysage
    }
    def add_paysage_id(row):
        etabli = row['new_lib']
        if pd.notna(etabli) and etabli.upper() in mapping:
            return mapping[etabli.upper()]
        else:
            return np.nan

    meef['id_paysage_formens'] = meef.apply(add_paysage_id, axis=1)
    meef = meef[['rentree', 'source', 'etabli', 'etabli_diffusion', 'flag_meef', 'new_lib', 'id_paysage_formens']]

    meef_null = meef.loc[meef.id_paysage_formens.isnull()].drop(columns='id_paysage_formens')
    if len(meef_null)>0:
        meef_null.to_csv(f"{PATH}work/meef_null.csv", sep=";", index=False)
        print("- ATTENTION ! il manque des etabli_diffusion dans la googleshit ; vérifier dans work/meef_null")
    return meef


def verifier_code_uai_safe(code_uai):
    alphabet_23 = ["a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "l", "m", "n", "p", "r", "s", "t", "u", "v", "w", "x", "y", "z"]
    
    if isinstance(code_uai, str):
        code_uai_propre = code_uai.strip().lower()
    else:
        print(code_uai)
        
    # Vérifier si les 7 premiers caractères sont numériques
    if len(code_uai_propre) < 8 or not code_uai_propre[:7].isdigit():
        return False
    
    code_uai_propre_chiffres = code_uai_propre[:7]
    code_uai_propre_lettre = code_uai_propre[7:8]
    
    try:
        rang_calcule = int(code_uai_propre_chiffres) % 23
        lettre_calculee = alphabet_23[rang_calcule]
        return code_uai_propre_lettre == lettre_calculee
    except ValueError:
        return False
class CustomInterrupt(Exception):
    pass


def etab_in_bcn():
    N_BCE_SISE_N = BCN['N_BCE_SISE_N']
    N_BCE_SISE_N[["NUMERO_UAI","LIBELLE_LONG_NATURE_UAI"]]
    N_BCE_SISE_N["compos"]=N_BCE_SISE_N["NUMERO_UAI"]
    N_BCE_SISE_N["rattach"]=N_BCE_SISE_N["NUMERO_UAI"]
    N_BCE_SISE_N["etabli"]=N_BCE_SISE_N["NUMERO_UAI"]
    N_BCE_SISE_N["lib_compos"]=N_BCE_SISE_N["APPELLATION_OFFICIELLE_UAI"]
    N_BCE_SISE_N["lib_rattach"]=N_BCE_SISE_N["APPELLATION_OFFICIELLE_UAI"]
    N_BCE_SISE_N["lib_etabli"]=N_BCE_SISE_N["APPELLATION_OFFICIELLE_UAI"]
    return N_BCE_SISE_N

def rattach():
    data_rattach = []
    for rentree in range(2001, 2023):
        # lecture format
        df_format, meta_format = pyreadstat.read_sas7bcat(f'format/inscri{str(rentree)[2:4]}/formats.sas7bcat',
                                                        encoding='iso-8859-1')
        for compos in meta_format.value_labels['$RATTACH']:
            rattach = meta_format.value_labels['$RATTACH'][compos]
            data_rattach.append({'rentree': rentree, 'compos': compos, 'rattach': rattach})
    for rentree in range(2023, 2025):
        df = pd.read_parquet(f'format/inscri{str(rentree)[2:4]}/bce_a24.parquet')
        for index, row in df.iterrows():
            data_rattach.append({'rentree': rentree, 'compos': row['numero_uai'], 'rattach': row['composante_rattachement']})
    df_rattach = pd.DataFrame(data_rattach)
    df_rattach[df_rattach['rattach'].isna() | (df_rattach['rattach'] == '')]

    return df_rattach










# def uai_paysage(df, var):
#     x=(df.merge(pd.DataFrame(PAYSAGE_id)[['id_value', 'id_paysage', 'active','id_enddate']], 
#                 how='left', left_on=var, right_on='id_value')
#         .drop_duplicates()
#         .merge(BCN['N_BCE_SISE_N'][['numero_uai', 'denomination_principale_uai']].drop_duplicates(), 
#                how='left', left_on=var, right_on='numero_uai'))

#     x=x.assign(paysage_presence=np.where(x.id_value.isnull(), 'N', 'Y'), variable=var)
#     x.loc[~x.id_enddate.isnull(), 'end_year'] = x.loc[~x.id_enddate.isnull()].id_enddate.str.split('-').str[0]
#     x['pid_multi']=x.groupby(['rentree', 'source', var]).transform('size')
#     return x.drop(columns=['numero_uai', 'id_value'])


# # def etablissements(year):
# #     from modules.variables_check import uai_paysage
# #     uai = pd.read_pickle(f"{PATH}output/uai_frequency_source_year{year}.pkl",compression= 'gzip')
# #     uai_pays = pd.DataFrame()
# #     up = uai_paysage(uai, 'compos')
# #     uai_pays = pd.concat([uai_pays, up], ignore_index=True)
# #     up = uai_paysage(uai, 'etabli')
# #     up = up.loc[~up.etabli.isin(uai_pays.compos.unique())]
# #     uai_pays = pd.concat([uai_pays, up], ignore_index=True)

# #     with pd.ExcelWriter(f"{PATH}vars_review_{year}.xlsx", mode='w', engine="openpyxl") as writer:  
# #         uai_pays.to_excel(writer, sheet_name='uai_paysage', index=False,  header=True, na_rep='')