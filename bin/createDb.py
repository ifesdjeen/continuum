import json, requests

url = 'http://localhost:3000/dbs'

params = dict(
    name='dbname',
    schema='[[\"a\",\"DbtLong\"]]'
)

resp = requests.post(url=url, params=params)

print resp.text
