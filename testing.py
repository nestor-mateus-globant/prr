import json
import requests

URL = "https://msiikyip82.execute-api.us-east-1.amazonaws.com/production/lambda"
PARAMS = { 'type': 'ListWorkSpaces'}
headers = {"content-type": "application/json",'Accept':'application/json'}

print('aaaaa')


DATA = {    
    "host" : "pwbi.cht9tueeuxkp.us-east-1.rds.amazonaws.com",
    "uname": "admin",
    "pwd"  : "evilking",
    "db"   : "pwbi",
    "where" :  {"client_id": "10a31dfa-4e65-4464-b0ee-3e0766c6fcff"}  
}

r = requests.post(url = URL, json = DATA,headers = headers,params = PARAMS)
data = r.json()

print(data)