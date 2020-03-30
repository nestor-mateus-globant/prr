import adal
import requests


authority_url = 'https://login.windows.net/common'
resource_url = 'https://analysis.windows.net/powerbi/api'

client_id = "635b8454-8c5c-4ba6-bf72-34abd9f96388"
username  = "admon@fna.adminforeport.com"
password  = "PwI4m=0.fN4O"

context = adal.AuthenticationContext(authority=authority_url,
                                     validate_authority=True,
                                     api_version=None)

token = context.acquire_token_with_username_password(resource=resource_url,
                                                     client_id=client_id,
                                                     username=username,
                                                     password=password)
access_token = token.get('accessToken')

print(access_token)


urls      =  ["6b054a04-eb52-4a35-8e07-73cb9e6501a0"]
group_id  = "3bad8e53-59ed-4b04-adf0-e761ebbebb2a"

for url in urls :
    refresh_url = 'https://api.powerbi.com/v1.0/myorg/groups/'+group_id+'/datasets/'+url +'/refreshes'
    #header  = {'Authorization': f'Bearer {access_token}'}
    header = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    r = requests.post(url=refresh_url, headers=header)
    r.raise_for_status()