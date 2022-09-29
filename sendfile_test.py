import json
import requests
from sqlalchemy import create_engine
mydb2 = create_engine("mysql+pymysql://test:asfTTsCVGs&%$23a@127.0.0.1:3306/testdb")
#example data
data = '''{
"hired_employees":
         [
          { "id": 2000, "name": "Starla Wreiford", "datetime" : "2022-10-12T01:14:30Z", "department_id" : 8, "job_id" : 100},
          { "id": 2001, "name": "Shaina Wyldish", "datetime" : "2023-01-24T14:38:23Z", "department_id" : 4, "job_id" : 69},
          { "id": 2002, "name": "Danya Wyre", "datetime" : "2021-01-13T12:54:26Z", "department_id" : 14, "job_id" : 186}
         ],
"jobs":[
{"id": 184,"job": "Administrative Assistant V"},
{"id": 185,"job": "Statistician V"},
{"id": 186,"job": "Production Engineer"}
],
"departments":[
{"id": 13,"department": "Customer services"},
{"id": 14,"department": "Production"}
]
}'''
dict = json.loads(data)

url = "http://127.0.0.1:49199/test_api"

file = ('data', json.dumps(data), 'application/json')
r = requests.post(url, json=dict)
#r2 =requests.post(url, files = (''))
print(r)