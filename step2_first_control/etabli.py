def etabli_paysage(year, paysage_id):
    from config_path import PATH

    import pandas as pd, numpy as np

    tmp=pd.read_pickle(f"{PATH}output/frequency_etabli_source_year{year}.pkl",compression= 'gzip')
    print(f"- size etabli {len(tmp)}")

    
    tmp=tmp.merge(paysage_id[['id_value','id_paysage','active','id_enddate']], how='left', left_on='etabli', right_on='id_value').drop_duplicates()

    tmp=tmp.assign(paysage_presence=np.where(tmp.id_value.isnull(), 'N', 'Y'))
    tmp.loc[~tmp.id_enddate.isnull(), 'end_year'] = tmp.loc[~tmp.id_enddate.isnull()].id_enddate.str.split('-').str[0]
    tmp['pid_multi']=tmp.groupby(['rentree', 'source', 'etabli', 'compos']).transform('size')


    print(f"- size etabli+paysage {len(tmp)}")
    return tmp