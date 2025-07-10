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