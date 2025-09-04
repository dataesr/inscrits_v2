from config_path import PATH
global CORRECTIFS_dict, BCN, PAYSAGE_id

def vars_no_empty(df):
    import json
    CONF=json.load(open('utils/config_sise.json', 'r'))
    no_empty = [i['var_sise'] for i in CONF if i['empty']=='NO']

    for c in no_empty:
        print(f"- {c} -> {df[c].unique()}")

    print("- INSPR clean")
    try:
        df.loc[df.inspr=='0', 'inspr'] = 'O'
    except:
        print("#### ATTENTION - INSPR not in df")


    print("- EFFECTIF clean")
    try:
        if any(~df.effectif.isin([0,1])):
            print("#### ATTENTION ! EFFECTIF not valid item")
    except KeyError:
        print("#### ATTENTION ! EFFECTIF not in df")    


    print("- AGE clean")
    try:
        liste_entiers = [x.astype('Int64') for x in df.age.unique()]
        liste_triee = sorted(liste_entiers)
        # Vérifications
        valeur_min = min(liste_triee)
        valeur_max = max(liste_triee)

        if valeur_min<14 or valeur_max>99:
            print(f"### ATTENTION ! age min < {valeur_min}, age max > {valeur_max}")
    # Tri de la liste
    except ValueError:
            print(f"### {[str(x).isdigit() and x != 0 for x in liste_entiers]}")

    return df


def data_cleansing(last_data_year, rentree, etab, meef):
    from config_path import PATH
    import pandas as pd, zipfile

    with zipfile.ZipFile(f"{PATH}output/sise_parquet_{last_data_year}.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'sise{str(rentree)[2:4]}.parquet'), engine='pyarrow')

    print(f"- {rentree} -> size: {len(df)}")

    # ETABLI & COMPOS & RATTACH
    if 'rattach' in df.columns:
        df = df.drop(columns='rattach')
    if 'etabli' in df.columns and 'compos' in df.columns:
        df = df.rename(columns={'etabli':'etabli_old', 'compos':'compos_old'})
        df = (df.merge(etab[['rentree', 'etabli_old', 'etabli', 'compos_old', 'compos', 'rattach']], 
                    how='left', on=['rentree', 'etabli_old', 'compos_old'])
            )
    df = df.loc[:, ~df.columns.str.contains('_old')]

    # ETABLI_DIFFUSION
    try:
        df = df.merge(meef, how='left').drop(columns='etabli_diffusion').rename(columns={'new_lib':'etabli_diffusion'})
    except KeyError:
        return print(f'no etabli_diffusion into sise {rentree}')

    df = vars_no_empty(df)

    
    return df

