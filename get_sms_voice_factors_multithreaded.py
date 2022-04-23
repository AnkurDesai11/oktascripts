import datetime, pandas, queue, requests, time, threading, urllib3
    
base_url = input("Enter Okta tenant base url: ")
api_token = input("Enter Okta api token: ")
input_file_path = input("OPTIONAL - Enter input filepath(absolute); DEFAULT - 'input_users.csv' in the same location as script: ") or "input_users.csv"
gpid_to_continue_from = input("OPTIONAL - Enter user gpid to continue from (if previous operation incomplete, ensure correct output file locationentered); DEFAULT - entire input file used: ")
output_file_path = input("OPTIONAL - Enter filepath to save output (absolute); DEFAULT - 'user_factors.csv' in the same location as script: ") or "user_factors.csv"
number_of_threads = input("OPTIONAL - Enter number of threads to run; DEFAULT - 3 (MIN/MAX - 1/5): ") or 3

headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(api_token)}
output_columns = ["GPID","searchResult", "status", "id", "profile.login", "profile.AD_LDAP_Mapper", "profile.mobilePhone", "profile.countryCode", "profile.idx_countryName", "Factors", "MFA_Voice_Number", "MFA_SMS_Number"]
batch_list = pandas.DataFrame(columns = output_columns)
total_processed = 0

if(not(base_url.split("/", 1)[0] == "https:" or base_url.split("/", 1)[0] == "http:" )):
    base_url = "https://"+base_url
    #print(baseUrl)

input_user_list = pandas.read_csv(input_file_path, header=0, keep_default_na=False).iloc[:,0]

if number_of_threads not in [1,2,3,4,5]:
    number_of_threads = 3
    print("Invalid value for number of threads, continuing with",number_of_threads,"threads")

if not gpid_to_continue_from:
    batch_list.to_csv(output_file_path, index=False)
else:
    if gpid_to_continue_from in input_user_list.values:
        input_user_list = input_user_list[input_user_list[input_user_list == gpid_to_continue_from].index[0]+1:]
    else:
        print(gpid_to_continue_from, "not found, appending current output to", output_file_path)

execution_start = datetime.datetime.now()
print("Script execution started at:", execution_start)

#queue not iterable hence search in series, modify and then load in queue
input_user_list_size = len(input_user_list)
shared_queue = queue.Queue(maxsize=0)
[shared_queue.put(id) for id in input_user_list]
input_user_list = None
#shared_queue = queue.deque(input_user_list)
#print (type(shared_queue), shared_queue.qsize(), shared_queue.full())

thread_lock = threading.Lock()

urllib3.disable_warnings() #to suppress unverified https request warning

def update_progress(progress, message):
    print('\r[ {0}{1} ] {2}% {3}'.format('#' * int(progress/2), ' ' * int(50 - progress/2), progress, message),end="")

def worker_thread():
    user_details = {}
    rate_limit = 0
    rate_remaining = 0
    rate_reset = 0
    batch_list = pandas.DataFrame(columns = output_columns)
    current_uid=""
    while not shared_queue.empty():
        try:
            uid = shared_queue.get()
            global total_processed
            thread_lock.acquire()
            total_processed += 1
            thread_lock.release()
            if uid is None or uid == "":
                continue
            id = '0'*int(8-len(str(uid)))+str(uid) if int(8-len(str(uid))) > 0 else str(uid)
            current_uid = uid
            uid="\""+uid+"\""
            endpoint = "{}/api/v1/users?search=profile.email+eq+{}".format(base_url,uid)
            response = requests.get(endpoint, headers=headers, verify = False)
            limit = response.headers['x-rate-limit-limit']
            remain = response.headers['x-rate-limit-remaining']
            reset = response.headers['x-rate-limit-reset']
            if response.status_code==200:
                if(len(response.json()) != 0):
                    user_details = pandas.json_normalize(response.json(), errors='ignore')
                    current_user_details = pandas.DataFrame([[current_uid, "Found",
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

                    batch_list = pandas.concat([batch_list,current_user_details], ignore_index=True)
                else:
                    #current_user_details = pandas.DataFrame([[current_uid, "Not Found", "","","","","","","","","",""]],
                    #    columns= output_columns)
                    batch_list = pandas.concat(
                        [batch_list, pandas.DataFrame([[current_uid, "User not found", "","","","","","","","","",""]], columns=output_columns)],
                        ignore_index=True)
            else:
                batch_list = pandas.concat(
                    [batch_list, pandas.DataFrame([[current_uid, "Unexpected response from users API", "","","","","","","","","",""]], columns=output_columns)],
                    ignore_index=True)

            if len(batch_list.index) >= 50:
                thread_lock.acquire()
                batch_list.to_csv(output_file_path, index=False, header=False, mode='a')
                thread_lock.release()
                batch_list = batch_list[0:0]
                update_progress( int((total_processed / input_user_list_size)*100) , "Users written to file : "+str(total_processed)+"              " )
            else:
                update_progress( int((total_processed / input_user_list_size)*100) , "Users processed currently : "+str(total_processed)+"          " )

            if int(limit)-int(remain) > int(limit)*0.4:
                update_progress( int((total_processed / input_user_list_size)*100) , "Waiting for rate limit: "+
                                str( abs( int(reset) - int(time.time()) ) / 5 )+"s, "+
                                str(total_processed)+" done " )
                time.sleep(abs( int(reset) - int(time.time()) ) / 5 )

            shared_queue.task_done()
        except Exception as e:
            batch_list = pandas.concat(
                    [batch_list, pandas.DataFrame([[current_uid, "Runtime error", "","","","","","","","","",""]], columns=output_columns)],
                    ignore_index=True)
            shared_queue.task_done()
            print("Runtime Error: ",e)

    if len(batch_list.index) != 50:
        thread_lock.acquire()
        batch_list.to_csv(output_file_path, index=False, header=False, mode='a')
        thread_lock.release()

for i in range(number_of_threads):
    t = threading.Thread(target = worker_thread)
    t.start()

shared_queue.join()

execution_end = datetime.datetime.now()
print("\nScript execution completed at:", execution_end)
print("Total time taken for script execution:", (execution_end - execution_start))