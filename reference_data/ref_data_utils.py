from config_path import PATH_NOMEN
import pickle, json

# Variables globales pour chaque nomenclature
BCN = {}
CORRECTIFS_dict = {}
PAYSAGE_dict = {}

def get_ref_data():
    """Charge les fichiers JSON dans les variables globales."""
    global BCN, CORRECTIFS_dict, PAYSAGE_dict

    # Liste des fichiers et leurs variables associées
    fichiers = {
        'bcn.pkl': BCN,
        'correctifs.json': CORRECTIFS_dict,
        'paysage.json': PAYSAGE_dict,
    }

    for fichier, variable in fichiers.items():
        if fichier.startswith('bcn'):
            with open(f'{PATH_NOMEN}bcn.pkl', 'rb') as f:
                variable.update(pickle.load(f))
        else:
            with open(f"{PATH_NOMEN}{fichier}", 'r', encoding='utf-8') as f:
                variable.update(json.load(f))



def update_ref_data(ref):
    """
    Recharge un fichier JSON spécifique dans sa variable globale associée.
    :param nom_fichier: Nom du fichier JSON à recharger (ex: 'CORRECTIFS.json')
    """
    global BCN, CORRECTIFS_dict, PAYSAGE_dict

    # Liste des fichiers et leurs variables associées
    refs_name = {
        'bcn.pkl': BCN,
        'correctifs.json': CORRECTIFS_dict,
        'paysage.json': PAYSAGE_dict,
    }

    if ref in refs_name:
        with open(f"{PATH_NOMEN}{ref}", 'r', encoding='utf-8') as f:
            refs_name[ref].clear()  # Vide la variable avant de recharger
            if ref.startswith('bcn'):
                refs_name[ref].update(pickle.load(f))
            else:
                refs_name[ref].update(json.load(f))
        print(f"Le fichier {ref} a été rechargé.")
    else:
        print(f"Erreur : {ref} n'est pas un fichier valide.")






# Charger les nomenclatures au premier import
get_ref_data()
