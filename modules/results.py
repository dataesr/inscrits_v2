from modules.cleansing import *
from modules.enrichment import *
from utils.functions_shared import *

#################################################################################################################
### COMPLETE ####################################################################################################
def data_result(df, etab, meef):
    import pandas as pd, numpy as np

    # ETABLI & COMPOS & RATTACH
    df = df.drop(columns='rattach')
    df = df.rename(columns={'etabli':'etabli_ori_uai', 'compos':'compos_ori_uai'})

    et = etab[['rentree', 'etabli_ori_uai', 'etabli', 'compos_ori_uai', 'compos', 'rattach', 
                'id_paysage', 'lib_paysage', 'id_paysage_epe', 'id_paysage_iut', 'id_paysage_iut_campus',
                'id_paysage_iut_pole', 'id_paysage_ing', 'id_paysage_ing_campus']]

    df = (df.merge(et, how='left', on=['rentree', 'etabli_ori_uai', 'compos_ori_uai'])
            .rename(columns={'compos':'ui', 'rattach':'ur'})
        )
    df = df.loc[:, ~df.columns.str.contains('compos_ori_uai')]

    # remove etabli without id_paysage
    df.loc[df.id_paysage=='', 'id_paysage'] = np.nan
    if len(df.loc[df.id_paysage.isnull()])>0:
        x=df.loc[df.id_paysage.isnull(), ['rentree', 'source', 'etabli']].drop_duplicates()
        print(f"- etabli without id_paysage by source: {x.source.value_counts()}")
        df = df.loc[~((df.source.isin(['mana', 'culture', 'enq26bis']))&(df.id_paysage.isnull()))]
    
    print(f"- size after add etab but remove etabli without id_paysage: {len(df)}")

    df_size_ori = len(df)
    no_same_size(df_size_ori, df)

    # ETABLI_DIFFUSION
    try:
        df = df.merge(meef, how='left').drop(columns='etabli_diffusion').rename(columns={'new_lib':'etabli_diffusion'})
    except KeyError:
        return print(f'no etabli_diffusion into sise {df.rentree.unique()}')
    no_same_size(df_size_ori, df)
    ##########################
 
    effectif_clean(df)

    func_list = [
    inspr_clean,
    diplom_empty,
    diplom_to_vars_bcn,
    sectdis_clean,
    groupe_clean,
    niveau_clean,
    cursus_clean,
    curpar_clean,
    amena_clean,
    conv_clean,
    degetu_clean,
    ed_clean,
    exoins_clean,
    echang_clean,
    bac_clean,
    bac_regroup_clean,
    bac_loc_clean,
    situpre_clean,
    country_clean,
    pcs_clean,
    comins_clean,
    cometa_clean,
    niveau_retard_avance,
    dndu_enrich,
    lmd_enrich,
    ed_enrich,
    dutbut_enrich,
    communes_enrich,
    prox_enrich,
    deptoreg,
    country_enrich,
    nation_bac_add,
    mobilite_add,
    effectifs
    ]

    for func in func_list:
        df = func(df)
        no_same_size(df_size_ori, df)
    return df

