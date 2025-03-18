# inscrits_v2

- create a root file into config_path.py with path to project_folder (This file is ignored.)

    - PATH -> chemin vers le dossier général 'inscrits'
    - into 'inscrits' 
        * input
        * output

1/ MAIN_INIT
- export data_review_(year).xlsx to check data
    - list datasets
    - vars, source, rentree
    - vars : row number non nulls and nulls per source+rentree
- output
    - zip parquet_basic with one file per source+rentree and selected columns
    - items_by_vars lists all items per vars and frequency