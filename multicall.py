import pandas, requests

baseUrl = input("Enter Okta tenant base url (without http://): ")
filepath = input("Enter filepath (absolute): ")
apiToken = input("Enter Okta api token: ")

userList = pandas.read_csv(filepath, header=0, keep_default_na=False).iloc[:,0]
#print(userList)

headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(apiToken)}
userLogin = "janet.white@forces.gc.ca"

#for userLogin in userList:
#    if userLogin is None or userLogin == "":
#        continue
endpoint = "https://{}/api/v1/users?search=profile.email+eq+{}".format(baseUrl,userLogin)
print(endpoint)
response = requests.get(endpoint, headers=headers, verify = False)
if response.status_code==200:
    print(response.json()[0].profile.accountID)
print(response.status_code)