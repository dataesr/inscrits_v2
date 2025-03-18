# inscrits_v2

- créer un fichier config_path.py à la racine contenant le chemin vers les fichiers et dossiers du projet
Ce fichier n'est pas commit sur git.
* PATH -> chemin vers le dossier général 'inscrits'

1/ MAIN_INIT
- export data_review_(year).xlsx 
    - liste datasets
    - vars, source, rentree
    - vars : row number non nulls and nulls per source+rentree
- output
    - zip parquet_basic with one file per source+rentree and selected columns
    - items_by_vars lists all vars and items and frequency