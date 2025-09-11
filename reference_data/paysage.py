def get_paysageODS():
    import requests, pandas as pd, json
    from utils.config_api import ods_headers
    from config_path import PATH_NOMEN
    paysage_dict = {}
    datasets = [{"fr-esr-paysage_structures_identifiants": 'paysage_id'}]
    for dataset in datasets:
        setname, dictname = next(iter(dataset.items()))
        
        url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{setname}/exports/json"

        response = requests.get(url, headers=ods_headers)
        paysage_dict[dictname] = response.json()
    json.dump(paysage_dict, open(f'{PATH_NOMEN}paysage.json', 'w'))


def get_paysage(json_name):
    from config_path import PATH_NOMEN
    import json

    with open(f'{PATH_NOMEN}{json_name}.json', "r") as f:
        file = json.load(f)
    return file


