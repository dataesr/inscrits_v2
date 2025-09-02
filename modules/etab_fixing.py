from reference_data.ref_data_utils import reference_data_loader
from config_path import PATH
import pandas as pd, json, numpy as np, pyreadstat

global CORRECTIFS_dict, BCN, PAYSAGE_id
CORRECTIFS_dict = reference_data_loader('google_sheet')
BCN = reference_data_loader('bcn')
PAYSAGE_id = reference_data_loader('paysage_id')


def uai_fixing(df):
    uai_fix=json.load(open('patches/uai_fixes.json', 'r'))
    df['compos_new'] = None
    df['etabli_new'] = None

    # Appliquer les corrections
    for annee, fixes in uai_fix.items():
        annee_int = int(annee)
        for fix in fixes:
            if 'compos' in fix:
                mask_compos = (df['rentree'] == annee_int) & (df['compos'] == fix['compos'])
                df.loc[mask_compos, 'compos_new'] = fix['compos_new']
                df.loc[mask_compos, 'rattach'] = np.nan
            if 'etabli' in fix:
                mask_etabli = (df['rentree'] == annee_int) & (df['etabli'] == fix['etabli'])
                df.loc[mask_etabli, 'etabli_new'] = fix['etabli_new']
    return df


def code_uai_safe_checking(code_uai):
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


def rattach_fixing(year):
    rattach_df = pd.read_pickle(f"{PATH}output/uai_frequency_source_year{year}.pkl",compression= 'gzip')
    rattach_df = rattach_df[['rentree', 'source', 'etabli', 'compos', 'rattach']]

    rattach_df = uai_fixing(rattach_df)
    for i in ['etabli', 'compos']:
        rattach_df.loc[rattach_df[f"{i}_new"].isnull(), f"{i}_new"] = rattach_df.loc[rattach_df[f"{i}_new"].isnull(), i]

    rattach_df['etabli_valide'] = rattach_df['etabli_new'].apply(code_uai_safe_checking)
    rattach_df['compos_valide'] = rattach_df['compos_new'].apply(code_uai_safe_checking)


    rattach_patch=json.load(open('patches/rattach_patch.json', 'r'))
    mapping = (
        pd.DataFrame([
            {"rentree": int(rentree), "compos_new": compos, 'rattach':rattach}
            for rentree, dico in rattach_patch.items()
            for compos, rattach in dico.items()
        ])
    )
    # corrections rattachements vides
    rattach_df = rattach_df.merge(mapping, how='left', on=["rentree", "compos_new"], suffixes=('', '_y'))
    rattach_df.loc[(rattach_df['rattach'].isna()) | (rattach_df['rattach'] == ''), 'rattach'] = rattach_df.loc[(rattach_df['rattach'].isna()) | (rattach_df['rattach'] == ''), 'rattach_y']
    rattach_df.drop(columns='rattach_y', inplace=True)

    rattach_null = rattach_df[rattach_df['rattach'].isna() | (rattach_df['rattach'] == '')]
    if len(rattach_null)>0:
        print(f"- après maj RATTACH il en manque encore ; compléter le dict patches/rattach_patch.json:\n{rattach_null}")
        tmp=(
                rattach_null.groupby("rentree")
                .apply(lambda x: dict(zip(x["compos"], x["rattach"])))
                .to_dict()
            )
        for annee, valeurs in tmp.items():
            if str(annee) in rattach_patch:
                rattach_patch[str(annee)].update(valeurs)
            else:
                rattach_patch[str(annee)] = valeurs

        # Écrire le résultat dans le fichier
        with open('rattach_patch.json', 'w') as f:
            json.dump(rattach_patch, f, indent=4)


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