def sise_config():
    from config_path import PATH
    import pandas as pd

    t=pd.read_csv(f"{PATH}sise_config.csv", encoding='utf-8', na_values=' ', keep_default_na=False, sep=';', dtype='str')
    t.to_json('utils/config_sise.json', orient='records', compression='infer', indent=4)