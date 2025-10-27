from config_path import PATH
import numpy as np, pandas as pd
from utils.functions_shared import *
from reference_data.ref_data_utils import CORRECTIFS_dict


def create_od_synthese(df):
  
    etab = pd.DataFrame(CORRECTIFS_dict['C_ETABLISSEMENTS'])
    vn = cols_selected['od2_vars_num']

    df = df.mask(df=='')

    d0 = (df.loc[df['etablissement_id_paysage'].isin(etab.loc[etab['operateur_lolf_150']=='O', 'id_paysage'].unique())]
          .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_id_paysage', 'etablissement_lib'], dropna=False)[vn].sum()
          .reset_index()
    )

    mask = (~df['etablissement_compos_lib'].isnull())&(df['etablissement_id_paysage'] != df['etablissement_compos_id_paysage'])

    d02 = (df.loc[mask&(df['etablissement_id_paysage'].isin(etab.loc[etab['operateur_lolf_150']=='O', 'id_paysage'].unique()))]
           .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_id_paysage', 'etablissement_lib', 'etablissement_compos_id_paysage', 'etablissement_compos_lib'], dropna=False)[vn].sum()
           .reset_index()
           .drop(columns=['etablissement_id_paysage', 'etablissement_lib'])
           .rename(columns={'etablissement_compos_id_paysage':'etablissement_id_paysage', 'etablissement_compos_lib':'etablissement_lib'})
           .assign(Attention = "* Attention : doubles comptes, établissement-composante", etablissement_lib = df['etablissement_lib'] + " *")
    )

    d0 = pd.concat([d0, d02], ignore_index=True)
    d0.loc[d0['Attention'].isnull(), 'Attention'] = "Sans double compte des établissements-composantes pour les EPE"

    ve = cols_selected['etab_vars_synth']
    d00 = df[ve].drop_duplicates()

    d002 = (df.loc[mask,
                  ['rentree', 'etablissement_id_paysage', 'etablissement_lib', 'etablissement_compos_id_paysage', 'etablissement_compos_lib']].drop_duplicates()
            .drop(columns=['etablissement_id_paysage', 'etablissement_lib'])
            .rename(columns={'etablissement_compos_id_paysage':'etablissement_id_paysage', 'etablissement_compos_lib':'etablissement_lib'})
            .assign(etablissement_lib = df['etablissement_lib'] + " *"))
