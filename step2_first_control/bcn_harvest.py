def bcn():
    from config_path import PATH
    from utils.functions_shared import last_file_into_folder
    import os, zipfile, json, pandas as pd, re

    # Répertoire contenant les fichiers ZIP
    directory = f'{PATH}bcn/'
    zip_file_path = last_file_into_folder(directory, 'zip')

    # Dictionnaire pour stocker les DataFrames de chaque fichier .dat
    dataframes = {}

    # Ouvrir le fichier ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Parcourir chaque fichier dans le ZIP
        for file_info in zip_ref.infolist():
            # Vérifier si le fichier est un .dat
            if file_info.filename.endswith('.dat'):
                # Ouvrir le fichier .dat
                with zip_ref.open(file_info.filename) as file:
                    # Lire le contenu du fichier
                    content = file.read().decode('utf-8')
                    # Nettoyer le contenu en supprimant les caractères de contrôle non valides
                    content = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', content)
                    try:
                        # Parser le contenu JSON
                        json_data = json.loads(content)
                        # Extraire le nom de la variable
                        variable_name = json_data.get('query', {}).get('NAME', '')
                        # Extraire les données de la clé "donnees"
                        donnees = json_data.get('donnees', [])
                        # Ajouter le nom de la variable à chaque entrée de données
                        for entry in donnees:
                            entry['VARIABLE_NAME'] = variable_name
                        # Créer un DataFrame à partir des données
                        df = pd.DataFrame(donnees)
                        # Ajouter le DataFrame au dictionnaire avec le nom de la variable comme clé
                        dataframes[variable_name] = df
                    except json.JSONDecodeError as e:
                        print(f"Erreur lors du parsing du fichier {file_info.filename}: {e}")

    # Liste des noms des DataFrames
    dataframe_names = list(dataframes.keys())

    # Afficher la liste des DataFrames
    print("Liste des DataFrames :")
    for name in dataframe_names:
        print(name)

    return dataframes