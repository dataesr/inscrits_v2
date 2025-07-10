def vars_sise_to_be_check(year, bcn):
    from config_path import PATH
    import pandas as pd


    vars_sise = pd.read_pickle(f"{PATH}output/items_by_vars{year}.pkl", compression='gzip')

    # vs = vars_sise[['variable']].drop_duplicates()
    vs = vars_sise.variable.str.lower().unique()
    bcn_info = [{'bcn_df': key, 'vars_ref': df.columns.tolist()} for key, df in bcn.items()]
    # Create a DataFrame from the list of dictionaries
    bcn_vars = pd.DataFrame(bcn_info)
    # Explode the 'Columns' list into separate rows
    bcn_vars = bcn_vars.explode('vars_ref').reset_index(drop=True)
    bcn_vars['vars_ref'] = bcn_vars['vars_ref'].str.lower()
    filtre=bcn_vars.loc[bcn_vars.vars_ref.isin(vs)]
    # bcn_vars=bcn.merge(vs, how='inner', left_on='vars_ref', right_on='variable')
 
        


        # # vars_ref=pd.concat([vars_ref, pd.DataFrame({'bcn_data': name, 'vars_ref':bcn[name].columns})])

    ['acabac', 
     'age', 
     'anbac', 
     'annais', 'bac', 'bac_rgrp', 'cometa',
       'comins', 'compos', 'conv', 'curpar', 'cursus_lmd', 'cycle',
       'degetu', 'deprespa', 'dipder', 'diplom', 'effectif', 'etabli',
       'flag_sup', 'groupe', 'inspr', 'nation', 'nature', 'nbach', 'net',
       'niveau', 'niveau_d', 'niveau_f', 'paripa', 'pcspar', 'regime',
       'sectdis', 'sexe', 'situpre', 'specia', 'specib', 'specic',
       'typ_dipl', 'typrepa', 'voie', 'numed', 'depbac', 'amena',
       'etabli_diffusion', 'flag_meef', 'oppos', 'par_type', 'exoins',
       'bac_spe1', 'bac_spe2', 'libelle_intitule_1_diplom',
       'libelle_intitule_2_diplom']