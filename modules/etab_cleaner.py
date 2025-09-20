from reference_data.ref_data_utils import CORRECTIFS_dict, BCN
from config_path import PATH
import pandas as pd, json, numpy as np


def uai_bcn():
    b=BCN['N_BCE_SISE_N']
    for i in ['etabli', 'compos', 'rattach']:
        b[i]=b["numero_uai"]
        b[f"lib_{i}"]=b["appellation_officielle_uai"]
    return b

def uai_fixing(df):
    uai_fix=json.load(open('patches/uai_fixes.json', 'r'))
    df['compos_new'] = None
    df['etabli_new'] = None

    # Appliquer les corrections
    for annee, fixes in uai_fix.items():
        annee_int = int(annee)
        for fix in fixes:
            vin = list(fix.keys())[0]
            vout = list(fix.keys())[1]
            mask = (df['rentree'] == annee_int) & (df[vin] == fix[vin])
            df.loc[mask, vout] = fix[vout]
            df.loc[mask, 'rattach'] = np.nan
    return df


def uai_patching(etab, patch_dict):
    if patch_dict == 'compos_empty':
        uai_v = ['etabli', 'compos']
    if patch_dict == 'rattach_patch':
        uai_v = ['compos', 'rattach']
    if patch_dict == 'etabli_patch':
        uai_v = ['rattach', 'etabli']

    map_dict = json.load(open(f"patches/{patch_dict}.json", 'r'))
    mapping = (
            pd.DataFrame([
            {"rentree": int(rentree), uai_v[0]: vin, uai_v[1]:vout}
            for rentree, dico in map_dict.items()
            for vin, vout in dico.items()
            ])
        )
    
    etab = etab.merge(mapping, how='left', on=["rentree", uai_v[0]], suffixes=('', '_y'))

    if patch_dict == 'compos_empty':
        etab.loc[(etab[uai_v[1]].isna()) | (etab[uai_v[1]] == ''), uai_v[1]] = etab.loc[(etab[uai_v[1]].isna()) | (etab[uai_v[1]] == ''), f'{uai_v[1]}_y']
    else:
        etab.loc[~etab[f'{uai_v[1]}_y'].isnull(), uai_v[1]] = etab.loc[~etab[f'{uai_v[1]}_y'].isnull(), f'{uai_v[1]}_y']


    # uai_empty_patch_checking
    etab_null = etab.loc[(etab[uai_v[1]].isna()) | (etab[uai_v[1]] == '')]
    if len(etab_null)>0:
        print(f"- reste {uai_v[1]} ; compléter le dict patches/{patch_dict}.json:\n{etab_null[['rentree', uai_v[0], uai_v[1]]]}")
        tmp=(
                etab_null.groupby("rentree")
                .apply(lambda x: dict(zip(x[uai_v[0]], x[uai_v[1]])))
                .to_dict()
            )
        for annee, valeurs in tmp.items():
            if str(annee) in map_dict:
                map_dict[str(annee)].update(valeurs)
            else:
                map_dict[str(annee)] = valeurs

        # Écrire le résultat dans le fichier
        with open(f'patches/{patch_dict}.json', 'w') as f:
            json.dump(map_dict, f, indent=4)
    
    return etab.drop(columns=f'{uai_v[1]}_y')


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


def uai_invalid_fix(etab):
    for i in ['etabli', 'compos', 'rattach']:
        etab[f'{i}_valide'] = etab[i].apply(code_uai_safe_checking)

    df_invalides = etab[~etab['etabli_valide'] | ~etab['compos_valide'] | ~etab['rattach_valide']]
    df_invalides[['rentree','source','etabli', 'etabli_valide', 'compos', 'compos_valide', 'rattach', 'rattach_valide','effectif_tot']]
    
    # Extraire les codes UAI distincts invalides des colonnes 'etabli' et 'compos'
    invalid_etabli_codes = etab.loc[~etab['etabli_valide'], 'etabli'].unique()
    invalid_compos_codes = etab.loc[~etab['compos_valide'], 'compos'].unique()
    invalid_rattach_codes = etab.loc[~etab['rattach_valide'], 'rattach'].unique()

    # Combiner et obtenir les codes distincts
    invalid_uai_codes = list(set(invalid_etabli_codes).union(set(invalid_compos_codes)).union(set(invalid_rattach_codes)))
    print(f"- size UAI_INVALID:{len(invalid_uai_codes)}\n{invalid_uai_codes}")

    liste_filtree = [mot for mot in invalid_uai_codes if len(mot) < 4 or mot[3] != 'P']
    if len(liste_filtree)>0:
        masque = pd.DataFrame(False, index=etab.index, columns=etab.columns)
        for s in liste_filtree:
            masque = masque | etab.isin([s])

        # Filtrer le DataFrame original avec le masque
        resultat_df = etab[masque.any(axis=1)]
        print(f"- uai to fix in patches:\n{resultat_df[['rentree', 'etabli', 'etabli_valide', 'compos', 'compos_valide', 'rattach', 'rattach_valide']]}")

    uai_ref = uai_bcn() 
    for i in ['etabli', 'compos', 'rattach']:
        etab = pd.merge(etab, uai_ref[[i, f"lib_{i}"]], how='left', on=[i])
    
    return etab


def etab_update(year):

    etab = pd.read_pickle(f"{PATH}output/uai_frequency_source_year{year}.pkl",compression= 'gzip')
    # etab = etab_df[['rentree', 'source', 'etabli', 'compos', 'rattach', 'effectif_tot']]
    print(f"- first size ETAB: {len(etab)}")

    # UAI wrong -> ETABLI=ETABLI_OLD & COMPOS=COMPOS_OLD
    etab = uai_fixing(etab)
    for i in ['etabli', 'compos']:
        etab.loc[etab[f"{i}_new"].isnull(), f"{i}_new"] = etab.loc[etab[f"{i}_new"].isnull(), i]
        etab = etab.rename(columns={i: f"{i}_old", f"{i}_new": i})

    # COMPOS_NEW empty
    etab = uai_patching(etab, 'compos_empty')
    print(f"- size ETAB after cleaning COMPOS: {len(etab)}")

    # RATTACH empty or wrong
    etab = uai_patching(etab, 'rattach_patch')
    print(f"- size ETAB after cleaning RATTACH: {len(etab)}")

    # ETABLI wrong
    etab = uai_patching(etab, 'etabli_patch')
    print(f"- size ETAB after cleaning ETABLI: {len(etab)}")

    # check uai validity
    etab = uai_invalid_fix(etab)
    print(f"- size ETAB after uai_invalid: {len(etab)}")
    return etab


def etabli_meef(year):
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
    print(f"- size ETAB after meef process: {len(meef)}")

    # keep only inspe espe and add id_paysage (id_paysage_formens)
    meef = meef.loc[(meef.flag_meef=='1')&(meef.new_lib.str.lower().str.startswith(('inspe', 'espe'), na=False))]
    meef_paysage = CORRECTIFS_dict['ETABLI_DIFFUSION_ID']
    mapping = {
    item['in']: item['out']
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


def rattach_single_add(compos_uai, rattach_uai, range_years):

    tmp=(
        {
            str(annee): {compos_uai: rattach_uai}
            for annee in range_years
        }
    )

    print(tmp)

    map_dict = json.load(open("patches/rattach_patch.json", 'r'))

    for annee, d in tmp.items():
        if annee in map_dict:
            # Récupérer le premier élément (clé) de d
            premiere_cle = next(iter(d))
            if premiere_cle in map_dict[annee]:
                # Mettre à jour la valeur si la clé existe
                map_dict[annee][premiere_cle] = d[premiere_cle]
            else:
                # Ajouter le dictionnaire si la clé n'existe pas
                map_dict[annee].update(d)
        else:
            # Ajouter l'année et le dictionnaire si l'année n'existe pas
            map_dict[annee] = d
    
    map_dict_trie = {annee: map_dict[annee] for annee in sorted(map_dict.keys(), key=int)}

    with open('patches/rattach_patch.json', 'w') as f:
        json.dump(map_dict_trie, f, indent=4)