def get_paysageODS(dataset, json_name):
    import requests, pandas as pd, json
    from utils.config_api import ods_headers
    from config_path import PATH_NOMEN

    url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"

    response = requests.get(url, headers=ods_headers)
    result=response.json()
    json.dump(result, open(f'{PATH_NOMEN}{json_name}.json', 'w'))


def get_paysage(json_name):
    from config_path import PATH_NOMEN
    import json

    with open(f'{PATH_NOMEN}/{json_name}.json', "r") as f:
        file = json.load(f)
    paysage_id = file.copy()  
    return paysage_id


