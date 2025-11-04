# from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
import pandas as pd
from config_path import PATH
from utils.functions_shared import work_csv, replace_by_nan
from modules.cleansing import uai_fixing, uai_patching, uai_invalid_fix, comins_clean, cometa_clean
from modules.enrichment import from_uai_to_paysage, from_id_to_lib, d_epe_enrich, iut_enrich, ing_enrich


################################################################################################
def etab_update(year):

    etab = pd.read_pickle(f"{PATH}output/uai_frequency_source_year{year}.pkl",compression= 'gzip').drop_duplicates()
    print(f"- first size ETAB: {len(etab)}")

    # UAI wrong -> ETABLI=ETABLI_ORI_UAI & COMPOS=COMPOS_ORI_UAI
    etab = uai_fixing(etab)
    for i in ['etabli', 'compos']:
        etab.loc[etab[f"{i}_new"].isnull(), f"{i}_new"] = etab.loc[etab[f"{i}_new"].isnull(), i]
        etab = etab.rename(columns={i: f"{i}_ori_uai", f"{i}_new": i})

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


    # etab enrich stage
    print(f"- size ETAB before paysage: {len(etab)}")
    etab = from_uai_to_paysage(etab)
    print(f"- size ETAB after id_paysage: {len(etab)}")
    etab = from_id_to_lib(etab)
    print(f"- size ETAB after lib_paysage: {len(etab)}")
    etab = d_epe_enrich(etab)
    print(f"- size ETAB after enrich_d_epe: {len(etab)}")

    etab = iut_enrich(etab)
    print(f"- size ETAB after enrich_d_epe: {len(etab)}")
    etab = ing_enrich(etab)
    print(f"- size ETAB after enrich_d_epe: {len(etab)}")


    # communes code clean
    etab = cometa_clean(etab)
    print(f"- size ETAB after cometa_clean: {len(etab)}")
    etab = comins_clean(etab)
    print(f"- size ETAB after comins_clean: {len(etab)}")



    for i in ['id_paysage_epe', 'id_paysage_iut', 'id_paysage_iut_campus', 'id_paysage_iut_pole', 'id_paysage_ing', 'id_paysage_ing_campus']:
        etab[i] = replace_by_nan(etab[i])


    verif_multi_com = (etab
                       .loc[~((etab.source.isin(['mana', 'culture', 'enq26bis']))&(etab.id_paysage.isnull()))]
                       .groupby(['rentree', 'etabli_ori_uai', 'etabli', 'compos_ori_uai', 'compos', 'id_paysage_source', 'id_paysage'], dropna=False )
                       .filter(lambda g: g['cometa'].nunique() > 1 or g['comui'].nunique() > 1)
                       .reset_index()[
                        ['rentree', 'etabli_ori_uai', 'etabli', 'id_paysage', 'lib_paysage', 'compos_ori_uai', 'compos', 'cometa','comui']]
    )

    if len(verif_multi_com)>0:
        work_csv(verif_multi_com, 'verif_etab')
        print("- ATTENTION ! ++ com for the same etabli -> verif_etab.csv")

    return etab