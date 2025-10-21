from modules.cleansing import *
from modules.enrichment import *
#################################################################################################################
### COMPLETE ####################################################################################################
def data_result(df, etab, meef):
    import pandas as pd, numpy as np

    # df = delete(df)

    # ETABLI & COMPOS & RATTACH
    df['etabli_ori_uai'] = df['etabli']
    df = df.drop(columns='rattach')
    df = df.rename(columns={'etabli':'etabli_orig', 'compos':'compos_orig'})
    df = (df.merge(etab[['rentree', 'etabli_orig', 'etabli', 'compos_orig', 'compos', 'rattach', 'id_paysage', 'lib_paysage', 'id_paysage_epe']], 
                how='left', on=['rentree', 'etabli_orig', 'compos_orig'])
            .rename(columns={'compos':'ui', 'rattach':'ur'})
        )
    df = df.loc[:, ~df.columns.str.contains('_orig')]

    # remove etabli without id_paysage
    df.loc[df.id_paysage=='', 'id_paysage'] = np.nan
    if len(df.loc[df.id_paysage.isnull()])>0:
        x=df.loc[df.id_paysage.isnull(), ['rentree', 'source', 'etabli']].drop_duplicates()
        print(f"- etabli without id_paysage by source: {x.source.value_counts()}")
        df = df.loc[~((df.source.isin(['mana', 'culture', 'enq26bis']))&(df.id_paysage.isnull()))]
    
    print(f"- size after remove etabli without paysage: {len(df)}")

    # ETABLI_DIFFUSION
    try:
        df = df.merge(meef, how='left').drop(columns='etabli_diffusion').rename(columns={'new_lib':'etabli_diffusion'})
    except KeyError:
        return print(f'no etabli_diffusion into sise {df.rentree.unique()}')
    
    ##########################
    # df = df.mask(df=='')

    effectif_clean(df)

    df = inspr_clean(df)
    df = diplom_empty(df)
    df = diplom_to_vars_bcn(df)
    df = sectdis_clean(df)
    df = groupe_clean(df)
    df = niveau_clean(df)
    df = cursus_clean(df)
    df = curpar_clean(df)
    df = amena_clean(df)
    df = conv_clean(df)
    df = degetu_clean(df)
    df = ed_clean(df)
    df = exoins_clean(df)
    df = echang_clean(df)
    
    df = bac_clean(df)
    df = bac_regroup_clean(df)
    df = dep_clean(df)
    df = aca_clean(df)
    df = situpre_clean(df)
    df = country_clean(df)
    df = pcs_clean(df)

    df = comins_clean(df) # COMUI
    df = cometa_clean(df)

    df = fresq_enrich(df)
    df = discpli_enrich(df)
    df = niveau_retard_avance(df)
    df = dndu_enrich(df)
    df = lmd_enrich(df)
    df = ed_enrich(df)
    df = iut_enrich(df)
    df = ing_enrich(df)
    df = dutbut_enrich(df)
    df = communes_enrich(df)
    df = prox_enrich(df)
    df = deptoreg(df)
    df = country_enrich(df)
    df = nation_bac_add(df)
    df = mobilite_add(df)
    df = effectifs(df)
    
    return df

