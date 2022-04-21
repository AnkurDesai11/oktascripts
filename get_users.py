import pandas, requests, urllib3

baseUrl = input("Enter Okta tenant base url: ")
apiToken = input("Enter Okta api token: ")
groupId = input("OPTIONAL - Enter Okta group id (empty for ALL users): ")
#userId = input("OPTIONAL - Enter Okta user id to continue after (only if not starting fresh, ensure previous incomplete file is in same filepath): ")
userLimit = int(input("OPTIONAL - Enter maximum no of users that can be fetched in single call (users api - 200 / groups api - 1000): ") or 1000)
nextLink = input("OPTIONAL - Enter 'next'/'self' link if available (group id/call limit if entered above will be ignored if link provided): ")
filePath = input("OPTIONAL - Enter filepath to save output (absolute): ")

if(not(baseUrl.split("/", 1)[0] == "https:" or baseUrl.split("/", 1)[0] == "http:" )):
    baseUrl = "https://"+baseUrl
    #print(baseUrl)

headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(apiToken)}
finalList = pandas.DataFrame(columns=["id", "profile.accountId", "profile.login", "profile.email", "profile.firstName", "profile.lastName"])
currentResponse = []
response = []
responseLength = 0
usersExtracted = 0
outputData = filePath+"\\userData_v2.csv" if filePath else "userData_v2.csv"
endpoint = ""

#if link provided check if next link the append to existing file, if first self link the write to new file
if nextLink:
    endpoint = nextLink
    if "groups" in nextLink:
        userLimit = 1000 if userLimit > 1000 else userLimit #recommended 200 for groups, will change from 1000 to 200
    else:
        userLimit = 200 if userLimit > 200 else userLimit
    if not("after=" in nextLink):
        finalList.to_csv(outputData, index=False)

#if link not provided, check if group users or all users, also check if starting after particular user
else:
    if groupId:
        userLimit = 1000 if userLimit > 1000 else userLimit
        endpoint = "{}/api/v1/groups/{}/users?limit={}".format(baseUrl,groupId,userLimit)
        finalList.to_csv(outputData, index=False)
        # if userId:
        #     endpoint = "{}/api/v1/groups/{}/users?limit={}&after={}".format(baseUrl,groupId,userLimit,userId)
        # else:
        #     finalList.to_csv(outputData, index=False)

    else:
        userLimit = 200 if userLimit > 200 else userLimit
        endpoint = "{}/api/v1/users?limit={}".format(baseUrl,userLimit)
        finalList.to_csv(outputData, index=False)
        # if userId:
        #     endpoint = "{}/api/v1/users?limit={}&after={}".format(baseUrl,userLimit,userId)
        # else:
        #     finalList.to_csv(outputData, index=False)

urllib3.disable_warnings() #to suppress unverified https request warning
try:
    response = requests.get(endpoint, headers=headers, verify = False)
    if response.status_code==200:
        responseLength = len(response.json())
    else:
        print("Unexpected Response from Okta:-",response.status_code," ",response.reason)
except  Exception as e:
    print("Error in calling API: "+str(e))

while responseLength > 0:
    #print (responseLength)
    try:
        currentResponse = pandas.json_normalize(response.json(), errors='ignore')
        print(currentResponse.tail())
        if(len(finalList.index)==0 or currentResponse.iloc[[-1]]["id"].values[0]!=finalList.iloc[-1]["id"]):
            finalList = pandas.concat([finalList,currentResponse[["id", "profile.accountId", "profile.login", "profile.email", "profile.firstName", "profile.lastName"]]])
            usersExtracted += responseLength
            print("Users extracted: ",usersExtracted)
            if (usersExtracted%5000 == 0):
                finalList.to_csv(outputData, index=False, header=False, mode='a')
                finalList = finalList[0:0]
                print("Users written to file: ",usersExtracted)
        if(responseLength<userLimit):
            break
        else:
            # if not nextLink:
            #     if groupId:
            #         endpoint = "{}/api/v1/groups/{}/users?limit={}&after=1{}".format(baseUrl,groupId,userLimit,currentResponse.iloc[[-1]]["id"].values[0])
            #     else:
            #         endpoint = "{}/api/v1/users?limit={}&after=1{}".format(baseUrl,userLimit,currentResponse.iloc[[-1]]["id"].values[0])
            # else:
            endpoint = response.headers['link'].split(", <", 1)[1].split(">", 1)[0]
            print("rel=next Link: ",endpoint)
            response = requests.get(endpoint, headers=headers, verify = False)
            if response.status_code==200:
                responseLength = len(response.json())
            else:
                responseLength = 0
                print("Unexpected Response from Okta:-",response.status_code," ",response.reason)
    except  Exception as e:
        print("Error in processing: "+str(e))


finalList.to_csv(outputData, index=False, header=False, mode='a')
print("Users written to file: ",usersExtracted)
print("File saved as: ",outputData)