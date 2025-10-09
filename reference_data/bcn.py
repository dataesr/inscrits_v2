def bcn_harvest():
    from config_path import PATH
    from utils.functions_shared import last_file_into_folder
    import zipfile, json, pandas as pd, re

    # Répertoire contenant les fichiers ZIP
    directory = f'{PATH}bcn/'
    zip_file_path = last_file_into_folder(directory, 'zip', 'BCN_EXTRACT_530')

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


def bcn_complete():
    from config_path import PATH_NOMEN
    from reference_data.bcn import bcn_harvest
    import pandas as pd, pickle

    bcn1=bcn_harvest()

    variable_name = 'N_EXONERATIONS'
    df=pd.DataFrame({'EXOINS':['A1', 'A2', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8'], 'LIBELLE_LONG':[
    "Etudiant français ou communautaire et EXONERATIONS D’ETABLISSEMENT (EXONERATIONS TOTALES)",
    "Etudiant étranger communautaire et EXONERATIONS BOURSES DU GOUVERNEMENT FRANÇAIS (EXONERATIONS TOTALES)",
    "Etudiant étranger extracommunautaire hors PERIMETRE D’APPLICATION DES DROITS MAJORES (doctorant ou inscrit en 2018-2019 ou inscrit en CPGE ou réfugié ou membre de famille de l’UE ou résident de longue durée ou résidence fiscale depuis plus de deux ans)",
    "Etudiant étranger extracommunautaire et TARIF PLEIN",
    "Etudiant étranger extracommunautaire et EXONERATIONS D’AMBASSADES (EXONERATIONS PARTIELLES)",
    "Etudiant étranger extracommunautaire et EXONERATIONS BOURSES DU GOUVERNEMENT FRANÇAIS (EXONERATIONS TOTALES)",
    "Etudiant étranger extracommunautaire et EXONERATIONS D’ETABLISSEMENT (EXONERATIONS TOTALES)",
    "Etudiant étranger extracommunautaire et EXONERATIONS D’ETABLISSEMENT (EXONERATIONS PARTIELLES)",
    "Etudiant étranger extracommunautaire et EXONERATIONS DE PARTENARIAT AVEC UN ETABLISSEMENT ETRANGER OU PROGRAMMES COMMUNAUTAIRES OU INTERNATIONAUX D’ACCUEIL D’ETUDIANTS (Erasmus+,etc….) ou autre exonération hors plafond (empêché, à distance,… )… (EXONERATIONS TOTALES)",
    "Etudiant étranger extracommunautaire et EXONERATIONS DE PARTENARIAT AVEC UN ETABLISSEMENT ETRANGER OU PROGRAMMES COMMUNAUTAIRES OU INTERNATIONAUX D’ACCUEIL D’ETUDIANTS (Erasmus+,etc….) ou autre exonération hors plafond (empêché, à distance,… )(EXONERATIONS PARTIELLES)"]})
    bcn1[variable_name] = df

    variable_name = 'N_AMENA'
    df=pd.DataFrame({'AMENA':['1', '2', '3', '4', '5', '6'], 'LIBELLE_LONG':[
    "CURSUS AMENAGE",
    "SEMESTRIALISATION",
    "CESURE",
    "LICENCE EN AVANCE (LOI ORE)",
    "LICENCE ALLONGEE (LOI ORE)",
    "LICENCE 3 ANS AVEC COMPLEMENTS PARALLELES (LOI ORE)"]})
    bcn1[variable_name] = df

    variable_name = 'N_OPPOSITION'
    df=pd.DataFrame({'OPPOS':['O', 'N'], 'LIBELLE_LONG':[
    "Opposition à la diffusion des données",
    "Aucune opposition"]})
    bcn1[variable_name] = df

    variable_name = 'N_EFFECTIF'
    df=pd.DataFrame({'EFFECTIF':[0, 1], 'LIBELLE_LONG':[
    "Inscriptions secondaires",
    "Inscriptions principales"]})
    bcn1[variable_name] = df

    variable_name = 'N_MEEF'
    df=pd.DataFrame({'FLAG_MEEF':['0', '1'], 'LIBELLE_LONG':[
    "Autres",
    "Inscriptions en master MEEF et DU formation adaptée enseignement"]})
    bcn1[variable_name] = df

    variable_name = 'N_SUP'
    df=pd.DataFrame({'FLAG_SUP':['0', '1'], 'LIBELLE_LONG':[
    "Inscriptions validess",
    "Non valides"]})
    bcn1[variable_name] = df

    for i in bcn1:
        bcn1[i].columns=bcn1[i].columns.str.lower()

        if i=='N_DISCIPLINE_SISE':
            bcn1[i] = bcn1[i].rename(columns={'discipline_sise':'discipli'})
        
        if i in ['N_CONVENTION', 'N_NIVEAU_SISE']:
            first_col = bcn1[i].columns[0]
            # bcn1[i][first_col] = bcn1[i][first_col].astype(str).apply(lambda x: x.rjust(2, '0') if x.isnumeric() else x)
            mask=bcn1[i][first_col].astype(str).str.isnumeric()
            bcn1[i].loc[mask, first_col] = bcn1[i].loc[mask, first_col].astype(str).str.rjust(2, fillchar='0') 
            

    print(bcn1)

    with open(f'{PATH_NOMEN}bcn.pkl', 'wb') as file:
        pickle.dump(bcn1, file)
   

# def get_all_bcn():
#     from config_path import PATH_NOMEN
#     import pickle, pandas as pd

#     with open(f'{PATH_NOMEN}bcn.pkl', 'rb') as file:
#         bcn = pickle.load(file)
        
#     return bcn