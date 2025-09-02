from reference_data.ref_data_utils import reference_data_loader
from config_path import PATH
import pandas as pd, json, numpy as np, pyreadstat
import pyarrow as pa
from pyarrow.parquet import ParquetFile


global CORRECTIFS_dict, BCN, PAYSAGE_id
CORRECTIFS_dict = reference_data_loader('google_sheet')
BCN = reference_data_loader('bcn')
PAYSAGE_id = reference_data_loader('paysage_id')

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




    # mapping = (
    #     pd.DataFrame([
    #         {"rentree": int(rentree), "compos": compos, 'rattach':rattach}
    #         for rentree, dico in json.load(open('patches/rattach_patch.json', 'r')).items()
    #         for compos, rattach in dico.items()
    #     ])
    # )
    # # corrections rattachements vides
    # df_rattach = df_rattach.merge(mapping, how='left', on=["rentree", "compos"], suffixes=('', '_y'))
    # df_rattach.loc[(df_rattach['rattach'].isna()) | (df_rattach['rattach'] == ''), 'rattach'] = df_rattach.loc[(df_rattach['rattach'].isna()) | (df_rattach['rattach'] == ''), 'rattach_y']

    # if len(df_rattach[df_rattach['rattach'].isna() | (df_rattach['rattach'] == '')])>0:
    #     print(f"- après maj RATTACH manquant:\n{df_rattach[df_rattach['rattach'].isna() | (df_rattach['rattach'] == '')]}")


    # df_rattach.loc[len(df_rattach)] = [2009,'0383412C','0383412C']
    # df_rattach.loc[len(df_rattach)] = [2009,'0772710C','0772710C']
    # df_rattach.loc[len(df_rattach)] = [2014,'0011312W','0694121E']
    # df_rattach.loc[len(df_rattach)] = [2013,'0022017G','0022017G']
    # df_rattach.loc[len(df_rattach)] = [2018,'0031123E','0631381J']
    # df_rattach.loc[len(df_rattach)] = [2007,'0061917B','0061917B']
    # df_rattach.loc[len(df_rattach)] = [2016,'0062088M','0062088M']
    # df_rattach.loc[len(df_rattach)] = [2015,'0062088M','0062088M']
    # df_rattach.loc[len(df_rattach)] = [2019,'0062205P','0062205P']
    # df_rattach.loc[len(df_rattach)] = [2011,'0133479L','0131844J']
    # df_rattach.loc[len(df_rattach)] = [2016,'0133784T','0133784T']
    # df_rattach.loc[len(df_rattach)] = [2011,'0134013S','0134013S']
    # df_rattach.loc[len(df_rattach)] = [2017,'013P0001','013P0001']
    # df_rattach.loc[len(df_rattach)] = [2014,'0141689K','0141689K']
    # df_rattach.loc[len(df_rattach)] = [2013,'0141689K','0141689K']
    # df_rattach.loc[len(df_rattach)] = [2013,'0141721V','0141721V']
    # df_rattach.loc[len(df_rattach)] = [2014,'0141721V','0141721V']
    # df_rattach.loc[len(df_rattach)] = [2013,'0141856S','0141856S']
    # df_rattach.loc[len(df_rattach)] = [2014,'0141856S','0141856S']
    # df_rattach.loc[len(df_rattach)] = [2013,'0141857T','0141857T']
    # df_rattach.loc[len(df_rattach)] = [2014,'0141857T','0141857T']
    # df_rattach.loc[len(df_rattach)] = [2016,'0161128P','0161128P']
    # df_rattach.loc[len(df_rattach)] = [2024,'0301923C','0301923C']
    # df_rattach.loc[len(df_rattach)] = [2023,'031P0001','031P0001']
    # df_rattach.loc[len(df_rattach)] = [2023,'031P0002','031P0002']
    # df_rattach.loc[len(df_rattach)] = [2023,'031P0003','031P0003']
    # df_rattach.loc[len(df_rattach)] = [2013,'0332929E','0333298F']
    # df_rattach.loc[len(df_rattach)] = [2017,'0333407Z','0333407Z']
    # df_rattach.loc[len(df_rattach)] = [2014,'0341088Y','0342321N']
    # df_rattach.loc[len(df_rattach)] = [2012,'0341267T','0341267T']
    # df_rattach.loc[len(df_rattach)] = [2012,'0342260X','0342260X']
    # df_rattach.loc[len(df_rattach)] = [2023,'037P0001','037P0001']
    # df_rattach.loc[len(df_rattach)] = [2019,'0383546Y','0383546Y']
    # df_rattach.loc[len(df_rattach)] = [2016,'0441679L','0441679L']
    # df_rattach.loc[len(df_rattach)] = [2015,'0441679L','0441679L']
    # df_rattach.loc[len(df_rattach)] = [2012,'0442311Y','0442311Y']
    # df_rattach.loc[len(df_rattach)] = [2007,'0490073N','0490073N']
    # df_rattach.loc[len(df_rattach)] = [2006,'0492076R','0492076R']
    # df_rattach.loc[len(df_rattach)] = [2013,'0501513Y','0501513Y']
    # df_rattach.loc[len(df_rattach)] = [2014,'0501513Y','0501513Y']
    # df_rattach.loc[len(df_rattach)] = [2014,'0501528P','0501528P']
    # df_rattach.loc[len(df_rattach)] = [2013,'0501528P','0501528P']
    # df_rattach.loc[len(df_rattach)] = [2014,'0501700B','0501700B']
    # df_rattach.loc[len(df_rattach)] = [2013,'0501700B','0501700B']
    # df_rattach.loc[len(df_rattach)] = [2015,'0512115X','0512115X']
    # df_rattach.loc[len(df_rattach)] = [2016,'0512115X','0512115X']
    # df_rattach.loc[len(df_rattach)] = [2016,'0542461G','0542461G']
    # df_rattach.loc[len(df_rattach)] = [2011,'0572081C','0542493S']
    # df_rattach.loc[len(df_rattach)] = [2023,'0597165T','0597165T']
    # df_rattach.loc[len(df_rattach)] = [2014,'0610994Z','0610994Z']
    # df_rattach.loc[len(df_rattach)] = [2013,'0610994Z','0610994Z']
    # df_rattach.loc[len(df_rattach)] = [2013,'0610995A','0610995A']
    # df_rattach.loc[len(df_rattach)] = [2014,'0610995A','0610995A']
    # df_rattach.loc[len(df_rattach)] = [2014,'0611097L','0611097L']
    # df_rattach.loc[len(df_rattach)] = [2013,'0611097L','0611097L']
    # df_rattach.loc[len(df_rattach)] = [2014,'0611290W','0611290W']
    # df_rattach.loc[len(df_rattach)] = [2023,'0650048Z','0650048Z']
    # df_rattach.loc[len(df_rattach)] = [2016,'0673030E','0673030E']
    # df_rattach.loc[len(df_rattach)] = [2015,'0673030E','0673030E']
    # df_rattach.loc[len(df_rattach)] = [2004,'0692989Z','0691500F']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753457A','0753457A']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753469N','0753469N']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753470P','0753470P']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753480A','0753480A']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753495S','0753495S']
    # df_rattach.loc[len(df_rattach)] = [2004,'0753574C','0753574C']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753667D','0753667D']
    # df_rattach.loc[len(df_rattach)] = [2016,'0753781C','0753781C']
    # df_rattach.loc[len(df_rattach)] = [2012,'0755451T','0755451T']
    # df_rattach.loc[len(df_rattach)] = [2015,'0755839P','0755839P']
    # df_rattach.loc[len(df_rattach)] = [2019,'0755976N','0755976N']
    # df_rattach.loc[len(df_rattach)] = [2019,'0756036D','0756036D']
    # df_rattach.loc[len(df_rattach)] = [2017,'075P0003','075P0003']
    # df_rattach.loc[len(df_rattach)] = [2023,'0763586K','0763586K']
    # df_rattach.loc[len(df_rattach)] = [2023,'0791136F','0791136F']
    # df_rattach.loc[len(df_rattach)] = [2013,'0801774U','0801774U']
    # df_rattach.loc[len(df_rattach)] = [2013,'0801955R','0801955R']
    # df_rattach.loc[len(df_rattach)] = [2013,'0875076V','0875076V']
    # df_rattach.loc[len(df_rattach)] = [2013,'0911991V','0783634B']
    # df_rattach.loc[len(df_rattach)] = [2014,'0912341A','0912341A']
    # df_rattach.loc[len(df_rattach)] = [2019,'0921682D','0912381U'] #UAI erroné 
    # df_rattach.loc[len(df_rattach)] = [2017,'0921682D','0912381U'] #UAI erroné 
    # df_rattach.loc[len(df_rattach)] = [2018,'0921682D','0912381U'] #UAI erroné 
    # df_rattach.loc[len(df_rattach)] = [2014,'0922750P','0922750P']
    # df_rattach.loc[len(df_rattach)] = [2015,'0951576X','0951576X']
    # df_rattach.loc[len(df_rattach)] = [2017,'1380021Y','1380021Y']
    # df_rattach.loc[len(df_rattach)] = [2014,'9830690H','9830690H']
    # df_rattach.loc[len(df_rattach)] = [2019,'9830705Z','9830705Z']
    # df_rattach.loc[len(df_rattach)] = [2018,'9830705Z','9830705Z']
    # df_rattach.loc[len(df_rattach)] = [2019,'9830706A','9830706A']
    # df_rattach.loc[len(df_rattach)] = [2018,'9830706A','9830706A']
    # df_rattach.loc[len(df_rattach)] = [2018,'9830707B','9830707B']
    # df_rattach.loc[len(df_rattach)] = [2019,'9830707B','9830707B']
    # df_rattach.loc[len(df_rattach)] = [2016,'0694045X','0694045X']

    return df_rattach

