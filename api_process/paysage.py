def get_paysageODS(dataset):
    import requests, pandas as pd, json
    from api_process.config_api import ods_headers
    from config_path import PATH_NOMEN

    url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"

    response = requests.get(url, headers=ods_headers)
    result=response.json()
    json.dump(result, open(f'{PATH_NOMEN}paysage_id.json', 'w'))


def get_paysage_id():
    from config_path import PATH_NOMEN
    import json

    with open(f'{PATH_NOMEN}/paysage_id.json', "r") as f:
        file = json.load(f)
    paysage_id = file.copy()  
    return paysage_id


