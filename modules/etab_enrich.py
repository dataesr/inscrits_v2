from reference_data.ref_data_utils import CORRECTIFS_dict, BCN, PAYSAGE_dict
import pandas as pd
from utils.functions_shared import work_csv


def from_uai_to_paysage(etab):
    
    a_uai = pd.DataFrame(CORRECTIFS_dict['A_UAI'])
    a_uai['rentree'] = a_uai['rentree'].astype(int)
    tmp = a_uai.loc[a_uai.type=='inscri', ['rentree', 'source', 'etabli', 'id_paysage']].drop_duplicates()
    tmp['paysage_count'] = (tmp.groupby(['rentree', 'source', 'etabli'], dropna=False)['id_paysage']
                               .transform('count')
                            )
    if len(tmp.query('paysage_count>1'))>0:
        print(f"- ATTENTION ++ id_paysage for etabli_uai: {tmp.query('paysage_count>1')}")

    etab = etab.merge(tmp, how='left', on=['rentree', 'source', 'etabli'])

    id_paysage_null = etab.loc[etab.id_paysage.isnull(), ['rentree', 'source', 'etabli', 'lib_etabli', 'etabli_valide']].drop_duplicates()
    paysage_id = pd.DataFrame(PAYSAGE_dict['paysage_id'])
    id_paysage_null = id_paysage_null.merge(paysage_id.loc[paysage_id.id_type=='uai'], how='left', left_on='etabli', right_on='id_value')[
        ['rentree', 'source', 'etabli', 'etabli_valide', 'lib_etabli', 'id_paysage',
        'usualname', 'active', 'id_startdate', 'id_enddate']]
    if len(id_paysage_null)>0:
        print("- ATTENTION, create file uai_paysage_missing_into_A_UAI for updating A_UAI")
        work_csv(id_paysage_null, 'uai_paysage_missing_into_A_UAI')

    return etab.drop(columns='paysage_count')


def from_id_to_lib(etab):
    c_etab = pd.DataFrame([{'id_paysage': i['id_paysage'], 'lib_paysage': i['uo_lib_courant']} for i in CORRECTIFS_dict['C_ETABLISSEMENTS']]).drop_duplicates()
    c_etab['paysage_count'] = c_etab.groupby(['id_paysage'], dropna=False)['lib_paysage'].transform('count')
    if len(c_etab.query('paysage_count>1'))>0:
        print(f"- ATTENTION ++ lib_paysage for id_paysage: {c_etab.query('paysage_count>1')}")
        
    etab = etab.merge(c_etab, how='left', on='id_paysage')
    return etab.drop(columns='paysage_count')

def enrich_d_epe(df):
    epe =  pd.DataFrame(CORRECTIFS_dict['D_EPE'])
    epe['rentree'] = epe.rentree.astype(int)
    df = pd.merge(df, epe[['rentree', 'id_paysage', 'id_paysage_epe']], on=['rentree', 'id_paysage'], how='left')
    return df


def enrich_paysage(etab):
    print(f"- size ETAB before paysage: {len(etab)}")
    etab = from_uai_to_paysage(etab)
    print(f"- size ETAB after id_paysage: {len(etab)}")
    etab = from_id_to_lib(etab)
    print(f"- size ETAB after lib_paysage: {len(etab)}")
    etab = enrich_d_epe(etab)
    print(f"- size ETAB after enrich_d_epe: {len(etab)}")
    return etab