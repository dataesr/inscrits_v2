import pandas as pd, json

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


def last_file_into_folder(path, extension, prefix):
    import os
    files_list = [f for f in os.listdir(path) if f.endswith(f'.{extension}') and f.startswith(prefix)]

    # Trouver le fichier le plus récent
    latest_file = max(files_list, key=lambda f: os.path.getmtime(os.path.join(path, f)))

    # Chemin complet vers le fichier le plus récent
    print(f"Le fichier {extension} le plus récent est : {os.path.join(path, latest_file)}")
    return os.path.join(path, latest_file)


def last_year_into_zip(path, prefix):
    # Importer le chemin de configuration depuis un module externe
    import zipfile
    zip_file_path=last_file_into_folder(path, 'zip', prefix)
    # Ouvrir le fichier ZIP en mode lecture
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        #keep the 2 last charecters of sisename and add '20'
        year = max({int('20' + item) for item in set(
            filter(None, [i.split('.')[0][-2:] for i in z.namelist()])
        )})
    print(f"the zip last year is {year}")
    return year


def get_individual_source(zip_full_path, source, rentree):
    import pandas as pd, zipfile, pickle, os

    filename = f'{source}{str(rentree)[2:4]}'
    print(filename)

    with zipfile.ZipFile(zip_full_path, 'r') as z:
        file_list = z.namelist()
        matching_files = [f for f in file_list if f.startswith(filename)]
        file_to_load = matching_files[0]
        extension = os.path.splitext(file_to_load)[1][1:] 

        try:
            # Vérifie l'extension du fichier
            if extension == 'parquet':
                df = pd.read_parquet(z.open(file_to_load), engine='pyarrow')
                print(f"- {rentree} -> size: {len(df)}")
                return df
            elif extension in ('pkl', 'pickle'):
                df = pd.read_pickle(z.open(file_to_load), compression='gzip')
                print(f"- {rentree} -> size: {len(df)}")
                return df
            else:
                print(f"Format de fichier non supporté : {extension}")
                return None
        except Exception as e:
            print(f"Erreur lors du chargement du fichier {file_to_load}: {e}")
            return None


def work_csv(df, file_csv_name):
    from config_path import PATH
    import os
    PATH_WORK=f"{PATH}work/"

    if not os.path.exists(PATH_WORK):
    # Créer le dossier
        os.makedirs(PATH_WORK)
        print(f"Le dossier a été créé à l'emplacement : {PATH_WORK}")
    else:
        print(f"Le dossier existe déjà à l'emplacement : {PATH_WORK}")

    name = file_csv_name
    return df.to_csv(f"{PATH_WORK}{name}.csv", sep=';', na_rep='', encoding='utf-8', index=False)


def data_save_by_year(rentree, df, filename, zip_path):
    from config_path import PATH
    import os, zipfile, pickle

    if not os.path.exists(f'{PATH}output'):
        print("folder OUTPUT creates into DATA_PATH")
    # Create a new directory because it does not exist
        os.mkdir(f'{PATH}output')
        
    file_name = f'{filename}{str(rentree)[2:4]}.pkl'.encode('utf-8').decode('utf-8')
    df.to_pickle(file_name, compression='gzip')


    print(f"Creating the pickle-files by year {file_name} into zip in OUTPUT")
    
    with zipfile.ZipFile(zip_path, 'a') as z:
        z.write(file_name)
        
    # Delete the parquet file after adding it to the ZIP
    try:
        os.remove(file_name)
        print(f"Deleted: {file_name}")
    except Exception as e:
        print(f"Error deleting {file_name}: {e}")


def replace_by_nan(serie: pd.Series) -> pd.Series:
    import numpy as np
    """Remplace None et "" par np.nan dans une série pandas."""
    return serie.replace([None, ""], np.nan)


def no_same_size(df_size_ori, df):
    try:
        if df_size_ori != len(df):
            raise ValueError(f"ATTENTION ! La taille du DataFrame a changé de {df_size_ori - len(df)} lignes.")
    except ValueError as e:
        print(e)
        raise  # Relance l'exception pour arrêter le script


def yaml_file(df_cols, list_name: str):
    import yaml
    if type(df_cols) is list:
        cols_name = sorted(df_cols)
    else:
        cols_name = sorted(df_cols.tolist())
    with open("utils/variables_selection.yaml", "a") as file:
        yaml.dump({list_name: cols_name}, file, default_flow_style=False)


cols_selected = {}
def load_list_vars():
    import yaml
    with open("utils/variables_selection.yaml", "r")  as f:
        cols_selected.update(yaml.safe_load(f))
load_list_vars()



def rattach_single_add(compos_uai, rattach_uai, range_years):

    tmp=(
        {
            str(annee): {compos_uai: rattach_uai}
            for annee in range_years
        }
    )

    print(tmp)

    map_dict = json.load(open("patches/rattach_patch.json", 'r'))

    for annee, d in tmp.items():
        if annee in map_dict:
            # Récupérer le premier élément (clé) de d
            premiere_cle = next(iter(d))
            if premiere_cle in map_dict[annee]:
                # Mettre à jour la valeur si la clé existe
                map_dict[annee][premiere_cle] = d[premiere_cle]
            else:
                # Ajouter le dictionnaire si la clé n'existe pas
                map_dict[annee].update(d)
        else:
            # Ajouter l'année et le dictionnaire si l'année n'existe pas
            map_dict[annee] = d
    
    map_dict_trie = {annee: map_dict[annee] for annee in sorted(map_dict.keys(), key=int)}

    with open('patches/rattach_patch.json', 'w') as f:
        json.dump(map_dict_trie, f, indent=4)


def rename_variables(df, category):

    with open("utils/variables_rename.json", 'r', encoding='utf-8') as fichier:
        mapping_json = json.load(fichier)

    if category not in mapping_json:
        raise ValueError(f"La catégorie '{category}' n'existe pas dans le JSON.")

    mapping = mapping_json[category]
    mapping_filtre = {cle: val for cle, val in mapping.items() if cle in df.columns}

    return df.rename(columns=mapping_filtre)