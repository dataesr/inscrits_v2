import os
paysage_headers = {'Content-Type': 'application/json', 'X-Api-Key': os.environ.get('X-API-KEY')}
ods_headers = {"Authorization": f"apikey {os.environ.get('ODS_API')}"}