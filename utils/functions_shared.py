def get_sources(annee):
    # selection des sources existantes en fonction des années
    assert(annee >= 2004)
    sources = ['inscri', 'inge', 'priv']
    #sources = ['inge', 'priv']
    if 2004 <= annee <= 2007:
        sources.append('iufm')
    if annee > 2004:
        sources.append('ens')
    if annee > 2005:
        sources.append('mana')
    if annee > 2015:
        sources.append('enq26bis')
    if annee > 2016:
        sources.append('culture')
    return sources


def last_file_into_folder(path, extension):
    import os
    files_list = [f for f in os.listdir(path) if f.endswith(f'.{extension}')]

    # Trouver le fichier le plus récent
    latest_file = max(files_list, key=lambda f: os.path.getmtime(os.path.join(path, f)))

    # Chemin complet vers le fichier le plus récent
    print(f"Le fichier {extension} le plus récent est : {os.path.join(path, latest_file)}")
    return os.path.join(path, latest_file)


def last_year_into_zip(path):
    # Importer le chemin de configuration depuis un module externe
    import zipfile
    zip_file_path=last_file_into_folder(path, 'zip')
    # Ouvrir le fichier ZIP en mode lecture
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        #keep the 2 last charecters of sisename and add '20'
        year = max({int('20' + item) for item in set(
            filter(None, [i.split('.')[0][-2:] for i in z.namelist()])
        )})
    print(f"the zip last year is {year}")
    return year


def nomenclatures_load(nomen):
    if nomen.lower()=='bcn':
        from nomenclatures.bcn import get_all_bcn
        return get_all_bcn()
    if nomen.lower()=='paysage_id':
        from nomenclatures.paysage import get_paysage_id
        return get_paysage_id()
    if nomen.lower()=='google_sheet':
        from nomenclatures.google_sheet import get_all_correctifs
        return get_all_correctifs()


def get_individual_source(source, rentree):
    import pandas as pd, zipfile

    filename = f'{source}{str(rentree)[2:4]}'
    print(filename)

    from config_path import PATH
    with zipfile.ZipFile(f"{PATH}input/parquet_origine.zip", 'r') as z:
        df = pd.read_parquet(z.open(f'{filename}.parquet'), engine='pyarrow')

    return df

def work_csv(df, file_csv_name):
    from config_path import PATH
    import os
    PATH_WORK=f"{PATH}/work/"

    if not os.path.exists(PATH_WORK):
    # Créer le dossier
        os.makedirs(PATH_WORK)
        print(f"Le dossier a été créé à l'emplacement : {PATH_WORK}")
    else:
        print(f"Le dossier existe déjà à l'emplacement : {PATH_WORK}")

    name = file_csv_name
    return df.to_csv(f"{PATH_WORK}{name}.csv", sep=';', na_rep='', encoding='utf-8', index=False)