from config_path import PATH
import numpy as np, pandas as pd, copy
from utils.functions_shared import *
from reference_data.ref_data_utils import CORRECTIFS_dict, BCN


def cross_vars_effect(df, k, v):
            
    icol = df.columns[:2].tolist()
    tmp_transp = df.pivot_table(
                index=icol,
                columns=k,  # Utilise la colonne spécifiée par &var comme colonnes
                values=v,
                aggfunc='sum',
                dropna=False).reset_index()

    tmp_transp = tmp_transp.rename(columns=lambda x: f"{k}{x}" if x != icol[0] and x != icol[1] else x)
    return tmp_transp


def od_synthese_by_etab(df):
  
    print("### OD synthe ETAB ###")
    vn = cols_selected['od_vars_num_init']

    mask_op = (df['operateur_lolf_150']=='O')
    mask_id = (~df['etablissement_compos_id_paysage'].isnull())&(df['etablissement_id_paysage'] != df['etablissement_compos_id_paysage'])
    
    d0 = (df.loc[mask_op]
            .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_id_paysage'], dropna=False)[vn].sum()
            .reset_index()
            .assign(Attention = "Sans double compte des établissements-composantes pour les EPE")
    )

    d02 = (df.loc[mask_id]
            .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_compos_id_paysage'], dropna=False)[vn].sum()
            .reset_index()
            .assign(Attention = "* Attention : doubles comptes, établissement-composante")
    )
   
    # ve = cols_selected['etab_vars_synth']
    # d00 = df[ve].drop_duplicates()

    # d002 = (df.loc[mask_id,
    #               ['rentree', 'etablissement_id_paysage', 'etablissement_compos_id_paysage']].drop_duplicates()
    #         .drop(columns=['etablissement_id_paysage'])
    #         .rename(columns={'etablissement_compos_id_paysage':'etablissement_id_paysage'})
    #         )

    # d00 = pd.concat([d00, d002], ignore_index=True)


    vars_list = ['sexe', 'bac', 'bac_age', 'attrac_nat_dep_bac', 'attrac_intern', 'dn_de', 'diplome', 'cursus_lmd', 
                    'gd_discipline', 'discipline', 'sect_disciplinaire', 'mobilite_intern', 'degetu']
    
    for veff in ['effectif', 'eff_ss_cpge']:
        dtmp0 = copy.deepcopy(d0)
        dtmp02 = copy.deepcopy(d02)
        pairs = [(var, veff) for var in vars_list]

        for dim, eff in pairs:
            tmp = df.loc[mask_op, ['rentree', 'etablissement_id_paysage', dim, eff]]
            tmp_transp = cross_vars_effect(tmp, dim, eff)
            dtmp0 = pd.merge(dtmp0, tmp_transp, how='left', on=['rentree', 'etablissement_id_paysage'])

            tmp = df.loc[mask_id, ['rentree', 'etablissement_compos_id_paysage', dim, eff]]
            tmp_transp = cross_vars_effect(tmp, dim, eff)       
            dtmp02 = (pd.merge(dtmp02, tmp_transp, how='left', on=['rentree', 'etablissement_compos_id_paysage'])
            )
        
        synth = (pd.concat([dtmp0, 
                           dtmp02.rename(columns={'etablissement_compos_id_paysage':'etablissement_id_paysage'})], ignore_index=True)
                    .sort_values(['rentree', 'etablissement_id_paysage'])

                        )
        synth = rename_variables(synth, 'names_vars_num')

        if veff == 'effectif':
            file_name = 'synth'
        elif veff == 'eff_ss_cpge':
            file_name = 'synthbis'
        
        path_export= f'{PATH}opendata/{file_name}.txt'.encode('utf-8').decode('utf-8')
        synth.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)


def od_synthese_by_inspe(df):
    print("### OD synthe INSPE ###")

    df['form_ens_id_paysage'] = replace_by_nan(df['form_ens_id_paysage'])
    mask_inspe = ((~df.form_ens_id_paysage.isnull())&(df['operateur_lolf_150']=='O'))

    vn = list(set(cols_selected['od_vars_num_init']) - {'nbach', 'nbach_ss_cpge', 'eff_dei'})
    d0 = (df.loc[mask_inspe]
          .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_id_paysage', 'form_ens_id_paysage'], dropna=False)[vn].sum()
          .reset_index()
    )

    vars_list = ['sexe', 'bac', 'bac_age', 'attrac_nat_dep_bac', 'attrac_intern', 'gd_discipline', 'discipline', 'mobilite_intern', 'degetu']
    vlist = [(var, 'eff_ss_cpge') for var in vars_list]

    for dim, eff in vlist:
        tmp = df.loc[mask_inspe, ['rentree', 'form_ens_id_paysage', dim, eff]].sort_values(['rentree', 'form_ens_id_paysage'])
        tmp_transp = cross_vars_effect(tmp, dim, eff)
        d0 = pd.merge(d0, tmp_transp, how='left', on=['rentree', 'form_ens_id_paysage'])

    d0 = d0.rename(columns={'form_ens_id_paysage':'composante_id_paysage'})
    d0 = rename_variables(d0, 'names_vars_num')

    path_export= f'{PATH}opendata/synthinspe.txt'
    d0.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)


def od_synthese_for_paysage(df):
    print("### OD synthe PAYSAGE ###")

    vn = cols_selected['od_vars_num_init']

    dd = pd.DataFrame()

    mapping={
        'etablissement_compos_id_paysage': {'epe_id_paysage':'établissement-composante'},
        'form_ens_id_paysage': {'inspe_id_paysage':'ESPE-INSPE'},
        'id_paysage_ed': {'ed_id_paysage':'ED'},
        'id_paysage_ing': {'ing_id_paysage':'ING'},
        'id_paysage_ing_campus': {'ing_id_paysage':'Campus ING'},
        'id_paysage_iut': {'iut_id_paysage':'IUT'},
        'id_paysage_iut_campus': {'iut_id_paysage':'Campus IUT'},
        'id_paysage_iut_pole': {'iut_id_paysage': 'Pôle IUT'},
        'iut_id_paysage': {'iut_id_paysage':'DUT'}
        }

    vars_dict = {'sexe':'eff_ss_cpge', 
                    'bac':['eff_ss_cpge', 'nbach'], 
                    'bac_age':['eff_ss_cpge', 'nbach'], 
                    'attrac_nat_dep_bac':'eff_ss_cpge',
                    'attrac_intern':'eff_ss_cpge',
                    'dn_de':'eff_ss_cpge',
                    'diplome':'eff_ss_cpge',
                    'cursus_lmd':'eff_ss_cpge',
                    'gd_discipline':'eff_ss_cpge', 
                    'discipline':'eff_ss_cpge',
                    'mobilite_intern':'eff_ss_cpge',
                    'correspondance_iut':'eff_ss_cpge',
                    'degetu':'eff_ss_cpge'
                    } 
    vlist = [(dim, eff) for dim, vs in vars_dict.items() for eff in (vs if isinstance(vs, list) else [vs])]


    for kid, did in mapping.items():
        for newid, att in did.items():

            df[kid] = replace_by_nan(df[kid])
            mask_id = (~df[kid].isnull())
            if kid in ['etablissement_compos_id_paysage', 'id_paysage_ing', 'id_paysage_iut']:
                mask_id = mask_id & (df['etablissement_id_paysage']!=df[kid])
            elif kid in ['id_paysage_ing_campus']:
                mask_id = mask_id & (df['etablissement_id_paysage']!=df['id_paysage_ing']) & (df[kid]!=df['id_paysage_ing'])
            elif kid in ['id_paysage_iut_campus']:
                mask_id = mask_id & (df[kid]!=df['id_paysage_iut'])
            elif kid in ['id_paysage_iut_pole']:
                mask_id = mask_id & (df[kid]!=df['id_paysage_iut']) & (df['id_paysage_iut_pole']!=df['id_paysage_iut_campus']) 

            d02 = (df.loc[mask_id]
                .groupby(['rentree', 'annee_universitaire', 'annee', kid], dropna=False)[vn].sum()
                .reset_index()
            )

            for dim, eff in vlist:
                tmp = df.loc[mask_id, ['rentree', kid, dim, eff]]
                dt = cross_vars_effect(tmp, dim, eff)
                if eff == 'nbach':
                    dt = dt.rename(columns=lambda x: 'n' + x if x.startswith('bac') else x)
                d02 = pd.merge(d02, dt, how='left', on=['rentree', kid])

            d02[newid] = d02[kid]
            d02['etablissement_id_paysage'] = d02[kid]
            d02 = (d02
                    .drop(columns=kid)
                    .assign(Attention = f"* Attention : doubles comptes {att}")
            )

            dd = pd.concat([dd, d02], axis=0, ignore_index=True)

    # aggregation on global id_paysage
    d0 = (df
          .groupby(['rentree', 'annee_universitaire', 'annee', 'etablissement_id_paysage'], dropna=False)[vn].sum()
          .reset_index()
    )
    for dim, eff in vlist:
        tmp = df[['rentree', 'etablissement_id_paysage', dim, eff]]
        dt = cross_vars_effect(tmp, dim, eff)
        if eff == 'nbach':
            dt = dt.rename(columns=lambda x: 'n' + x if x.startswith('bac') else x)
        d0 = pd.merge(d0, dt, how='left', on=['rentree', 'etablissement_id_paysage'])
    
    dd = pd.concat([d0, dd], axis=0, ignore_index=True)
    dd = rename_variables(dd, 'names_vars_num')

    path_export= f'{PATH}opendata/synthpaysage.txt'.encode('utf-8').decode('utf-8')
    dd.to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)


def od_synthese_by_diplom(df):
    print("### OD synthe DIPLOM ###")

    va = cols_selected['od_vars_diplom']
    vn = list(set(cols_selected['od_vars_num_init']) - {'nbach', 'nbach_ss_cpge'})

    d0 = (df.groupby(va, dropna=False)[vn].sum()
          .reset_index())

    sex_dict = {'M':'hommes', 'F':'femmes'}
    for k, v in sex_dict.items():    
        d0.loc[d0['sexe'] == k, v] = d0['eff_ss_cpge']
            
    d0.loc[d0['lmddontbis'].isin(['DOCT', 'HDR']), 'niveau'] = 'XX'

    td = [item['typ_dipl'] for item in CORRECTIFS_dict['TYP_DIPL']]
    d0.loc[~d0['typ_dipl'].isin(td), 'niveau'] = 'XX'
    d0.loc[~d0['typ_dipl'].isin(td), 'diplome_sise'] = '9999999'
    d0.loc[~d0['typ_dipl'].isin(td), 'typ_dipl'] = '09'

    # licence pro
    d0.loc[(d0['diplome_sise'].isin(['2400073', '2400155']))&(d0['niveau']!='01'), 'niveau'] = '01'

    ds = ["9030632","9030418","9030136","4200069","9030816","9030634","9030906","9031107","9030627","9030629","9030815","9084513","9021229","9020129","9022270","9075609","9022241","9053311"]
    d0.loc[d0['diplome_sise'].isin(ds)] = '9999999'


    va = list(set(cols_selected['od_vars_diplom']) - {'sexe'})
    vn.extend(['femmes', 'hommes'])
    d0 = d0.groupby(va, dropna=False)[vn].sum().reset_index()

    d0 = rename_variables(d0, 'names_vars_num')
    d0 = d0.rename(columns={'eff_dei':'effectif_dei'})
    
    dip_lib = pd.DataFrame(BCN['N_DIPLOME_SISE'])[['diplome_sise', 'libelle_intitule_1', 'libelle_intitule_2']].rename(columns={'diplome_sise':'diplom'})
    dip_lib[['libelle_intitule_1', 'libelle_intitule_2']] = dip_lib[['libelle_intitule_1', 'libelle_intitule_2']].fillna("").apply(lambda x: x.str.strip().str.capitalize())

    typ = pd.DataFrame(CORRECTIFS_dict['TYP_DIPL'])
    
    d0 = (d0.merge(dip_lib, how='left', on='diplom')
            .merge(typ[['typ_dipl', 'typ_dipl_lib']], how='left', on='typ_dipl')
            )

    for i in ['diplom', 'typ_dipl', 'niveau', 'degetu']:
        d01 = d0.groupby([i], dropna=False)['effectif'].sum().reset_index()
        with pd.ExcelWriter(f"{PATH}work/diplom_check.xlsx", mode='a', if_sheet_exists='replace') as writer:  
            d01.to_excel(writer, sheet_name=i, index=False)

    d0 = d0.fillna('')

    for i in ['diplomes', 'diplomes_fresq']:
        if i=='diplomes':
            mask=(d0['operateur_lolf_150']=='O')
        else:
            mask=(d0['etablissement_id_paysage']!='')
        path_export = f'{PATH}opendata/{i}.txt'.encode('utf-8').decode('utf-8')
        d0.loc[mask].to_csv(path_export, encoding='utf-8', na_rep='', sep='\t', index=False)