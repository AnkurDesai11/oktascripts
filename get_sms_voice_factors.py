import pandas, requests, urllib3, time

def update_progress(progress, message):
    print('\r[ {0}{1} ] {2}% {3}'.format('#' * int(progress/2), ' ' * int(50 - progress/2), progress, message),end="")
    
base_url = input("Enter Okta tenant base url: ")
api_token = input("Enter Okta api token: ")
input_file_path = input("OPTIONAL - Enter input filepath(absolute); DEFAULT - 'input_users.csv' in the same location as script: ") or "input_users.csv"
gpid_to_continue_from = input("OPTIONAL - Enter user gpid to continue from (if previous operation incomplete, ensure correct output file locationentered); DEFAULT - entire input file used: ")
output_file_path = input("OPTIONAL - Enter filepath to save output (absolute); DEFAULT - 'user_factors.csv' in the same location as script: ") or "user_factors.csv"

input_user_list = pandas.read_csv(input_file_path, header=0, keep_default_na=False).iloc[:,0]

if(not(base_url.split("/", 1)[0] == "https:" or base_url.split("/", 1)[0] == "http:" )):
    base_url = "https://"+base_url
    #print(baseUrl)

headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(api_token)}
output_columns = ["GPID","searchResult", "status", "id", "profile.login", "profile.AD_LDAP_Mapper", "profile.mobilePhone", "profile.countryCode", "profile.idx_countryName", "Factors", "MFA_Voice_Number", "MFA_SMS_Number"]
final_list = pandas.DataFrame(columns = output_columns)
user_details = {}
total_processed = 0
rate_limit = 0
rate_remaining = 0
rate_reset = 0

urllib3.disable_warnings() #to suppress unverified https request warning

if not gpid_to_continue_from:
    final_list.to_csv(output_file_path, index=False)
else:
    if gpid_to_continue_from in input_user_list.values:
        input_user_list = input_user_list[input_user_list[input_user_list == gpid_to_continue_from].index[0]+1:]
    else:
        print(gpid_to_continue_from, "not found, appending current output to", output_file_path)

for user_login in input_user_list:
    try:
        total_processed += 1
        if user_login is None or user_login == "":
            continue
        user_login = '0'*int(8-len(str(user_login)))+str(user_login) if int(8-len(str(user_login))) > 0 else str(user_login)
        current_user_login = user_login
        user_login="\""+user_login+"\""
        endpoint = "{}/api/v1/users?search=profile.AD_LDAP_Mapper+eq+{}".format(base_url,user_login)
        response = requests.get(endpoint, headers=headers, verify = False)
        limit = response.headers['x-rate-limit-limit']
        remain = response.headers['x-rate-limit-remaining']
        reset = response.headers['x-rate-limit-reset']
        if response.status_code==200:
            if(len(response.json()) != 0):
                user_details = pandas.json_normalize(response.json(), errors='ignore')
                current_user_details = pandas.DataFrame([[current_user_login, "Found",
                    user_details["status"].values[0], user_details["id"].values[0], user_details["profile.login"].values[0], 
                    user_details["profile.AD_LDAP_Mapper"].values[0] if ("profile.AD_LDAP_Mapper" in user_details) else "", 
                    user_details["profile.mobilePhone"].values[0] if ("profile.mobilePhone" in user_details) else "", 
                    user_details["profile.countryCode"].values[0] if ("profile.countryCode" in user_details) else "", 
                    user_details["profile.idx_countryName"].values[0] if ("profile.idx_countryName" in user_details) else "",
                    "","",""
                    ]],
                    columns = output_columns)
                endpoint = "{}/api/v1/users/{}/factors".format(base_url,user_details["id"].values[0])
                response = requests.get(endpoint, headers=headers, verify = False)
                if response.status_code==200:
                    if(len(response.json()) != 0):
                        user_mfa_details = pandas.json_normalize(response.json(), errors='ignore')
                        current_user_details.at[0, 'Factors'] = ','.join(user_mfa_details['factorType'])
                        if 'call' in user_mfa_details.factorType.values:
                            current_user_details.at[0, 'MFA_Voice_Number'] = str(user_mfa_details[user_mfa_details['factorType'] == "call"]['profile.phoneNumber'].values[0])
                        if 'sms' in user_mfa_details.factorType.values:
                            current_user_details.at[0, 'MFA_SMS_Number'] = str(user_mfa_details[user_mfa_details['factorType'] == "sms"]['profile.phoneNumber'].values[0])
                    else:
                        current_user_details.at[0, 'searchResult'] = "MFA not found"
                else:
                    current_user_details.at[0, 'searchResult'] = "Unexpected response from factors API"
            
                final_list = pandas.concat([final_list,current_user_details], ignore_index=True)
            else:
                #current_user_details = pandas.DataFrame([[current_user_login, "Not Found", "","","","","","","","","",""]],
                #    columns= output_columns)
                final_list = pandas.concat(
                    [final_list, pandas.DataFrame([[current_user_login, "User not found", "","","","","","","","","",""]], columns=output_columns)],
                    ignore_index=True)
        else:
            final_list = pandas.concat(
                [final_list, pandas.DataFrame([[current_user_login, "Unexpected response from users API", "","","","","","","","","",""]], columns=output_columns)],
                ignore_index=True)
        
        if len(final_list.index) >= 50:
            final_list.to_csv(output_file_path, index=False, header=False, mode='a')
            final_list = final_list[0:0]
            update_progress( int((total_processed / len(input_user_list))*100) , "Users written to file : "+str(total_processed)+"              " )
        else:
            update_progress( int((total_processed / len(input_user_list))*100) , "Users processed currently : "+str(total_processed)+"          " )
        
        if int(limit)-int(remain) > int(limit)*0.5:
            update_progress( int((total_processed / len(input_user_list))*100) , "Waiting for rate limit: "+
            str( abs( int(reset) - int(time.time()) ) / 5 )+"s, "+
            str(total_processed)+" done "+str (abs( int(reset) - int(time.time()) ) / 5) )
            time.sleep(abs( int(reset) - int(time.time()) ) / 5 )
        
    except Exception as e:
        final_list = pandas.concat(
                [final_list, pandas.DataFrame([[current_user_login, "Runtime error", "","","","","","","","","",""]], columns=output_columns)],
                ignore_index=True)
        print("Runtime Error: ",e)

if len(final_list.index) != 50:
    final_list.to_csv(output_file_path, index=False, header=False, mode='a')