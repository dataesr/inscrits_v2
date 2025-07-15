def get_paysageODS(dataset):
    import requests, pandas as pd
    from api_process.config_api import ods_headers
    url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"

    response = requests.get(url, headers=ods_headers)
    result=response.json()
    return pd.DataFrame(result)


