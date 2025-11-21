from config_path import PATH_NOMEN
import json, pandas as pd, os

def get_all_correctifs_from_google():
    url=f"https://docs.google.com/spreadsheet/ccc?key={os.environ.get('GOOGLE_KEY')}&output=xls"

    CORRECTIFS_dict = {}
    VARS=['LES_COMMUNES', 'COMINS', 'COMETAB', 'A_UAI', 'C_ETABLISSEMENTS', 'ETABLI_DIFFUSION_ID', 
          'delete', 'GROUPE_CORRECTIF', 'O_INF_FRESQ', 'G_PAYS', 'CORRLMD', 'LMDDONTBIS',
          'CURSUS_LMD_CORRECTIF', 'A_COMETA', 'DEP_ACA_CORRECTIF', 'I_DNDU', 'J_LMDDONT', 'L_ED',
          'D_EPE', 'M_IUT', 'N_ING', 'O_DUTBUT', 'H_PROXIMITE', 'DEPTOREGNEW', 'DEPTOREG', 'SEXE',
          'BAC_RGRP', 'AVANCE_RETARD', 'PROXBAC', 'PROXREGBAC', 'ATTRAC_INTERN', 'MOBILITE_INTERN',
          'NIVEAU', 'DEGETU', 'SECTDIS', 'SPECIUT', 'DNDU', 'CURSUS_LMD', 'TYP_DIPL']
    df_c = pd.read_excel(url, sheet_name=VARS, dtype=str, na_filter=False)
    for VAR in VARS:
        # logger.debug(f"loading {VAR}...")
        correctifs = df_c.get(VAR).to_dict(orient='records')
        correctifs = [{k.lower(): v for k, v in d.items()} for d in correctifs]
        for c in correctifs:
            for f in c:
                if c[f] != c[f]: # nan
                    c[f] = ''
                if 'annee' in f.lower() or 'rentree' in f.lower():
                    c[f] = str(c[f])
                if isinstance(c[f], str) and f.lower() != 'coordonnees':
                    c[f] = c[f].split('.0')[0].strip()
                elif isinstance(c[f], float) or isinstance(c[f], int):
                    c[f] = str(c[f]).split('.0')[0].strip()
                   
        CORRECTIFS_dict[f'{VAR}'] = correctifs
    json.dump(CORRECTIFS_dict, open(f'{PATH_NOMEN}correctifs.json', 'w'))