import adal
import requests
import json


authority_url = 'https://login.windows.net/common'
#authority_url ='https://login.microsoftonline.com/common/oauth2/token'
resource_url = 'https://analysis.windows.net/powerbi/api'

client_id = "b9b2a09b-8dcb-4a00-9613-383faf73fec4"
username  = "admon@fna.adminforeport.com"
password  = "PwI4m=0.fN4O"
group_id  = "3bad8e53-59ed-4b04-adf0-e761ebbebb2a"
report_id = "1e2cb776-7095-45ff-9197-64ebf20150f4"


settings  = {
    "accessLevel": "View", 
    "allowSaveAs": "false"
    ,"identities": [{"Username": "admon@fna.adminforeport.com","roles": ["SYNERGIA"],
                     "datasets": [ "6b054a04-eb52-4a35-8e07-73cb9e6501a0" ]}]
}


context   = adal.AuthenticationContext(authority=authority_url,
                                     validate_authority=True,
                                     api_version=None)

token = context.acquire_token_with_username_password(resource=resource_url,
                                                     client_id=client_id,
                                                     username=username,
                                                     password=password)

access_token = token.get('accessToken')

print(access_token)

dest = 'https://api.powerbi.com/v1.0/myorg/groups/' + group_id \
       + '/reports/' + report_id + '/GenerateToken'

embed_url = 'https://app.powerbi.com/reportEmbed?reportId=' \
            + report_id + '&groupId=' + group_id 


headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
response = requests.post(dest, data=json.dumps(settings), headers=headers)
    

