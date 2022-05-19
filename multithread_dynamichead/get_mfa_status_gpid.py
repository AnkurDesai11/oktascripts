import datetime, logging, pandas, queue, requests, time, threading, urllib3

append_time = datetime.datetime.now().strftime("%d%b%Y_%H%M%S")
logging.basicConfig(filename=str("log"+append_time+".out"), format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

header_file_path = input("OPTIONAL - Enter header filepath(absolute); SKIP - enter 0 for headers id, status, profile.login; DEFAULT - 'all_headers.csv' in the same location as script: ") or "all_headers.csv"
selected_columns = []
if header_file_path == "0":
    selected_columns = ["id", "status", "profile.login"]
else:
    all_headers = list((pandas.read_csv(header_file_path, header=0)).columns.values)
    for i, item in enumerate(all_headers):
        print(i,"-",item, end=" , ")
    print()
    selected_headers = input("Enter columns(index) separated by space : ")
    selected_headers = [int(i) for i in selected_headers.split(" ")]
    selected_columns = [all_headers[i] for i in selected_headers]
    print("Selected columns are : ",",".join(selected_columns))

selected_columns[0:0] = ["uid", "result"]
################################################################
# CHANGE THE COLUMNS ASSIGNED BELOW TO OPERATION SPECIFIC VALUES
################################################################
#selected_columns[len(selected_columns):len(selected_columns)] = ["Factors", "MFA_Voice_Number", "MFA_SMS_Number"]

base_url = input("Enter Okta tenant base url: ")
api_token = input("Enter Okta api token: ")
input_file_path = input("OPTIONAL - Enter input filepath(absolute); DEFAULT - 'input_users.csv' in the same location as script: ") or "input_users.csv"
id_to_continue_from = input("OPTIONAL - Enter user id to continue from (if previous operation incomplete, ensure correct output file location entered); DEFAULT - entire input file used: ")
output_file_path = input("OPTIONAL - Enter filepath to save output (absolute); DEFAULT - 'user_factors_[timestamp].csv' in the same location as script: ") or "user_factors_"+append_time+".csv"
number_of_threads = input("OPTIONAL - Enter number of threads to run; DEFAULT - 5 (MIN/RECOMMENDED/MAX - 1/<10/15): ") or 5
max_rate_limit = input("OPTIONAL - Enter max rate limit to consume; DEFAULT - 0.4 (MIN/MAX - 0.2/0.6): ") or 0.4

headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(api_token)}
batch_list = pandas.DataFrame(columns = selected_columns)
total_processed = 0

if(not(base_url.split("/", 1)[0] == "https:" or base_url.split("/", 1)[0] == "http:" )):
    base_url = "https://"+base_url
    #print(baseUrl)

input_user_list = pandas.read_csv(input_file_path, header=0, keep_default_na=False).iloc[:,0]

try:
    number_of_threads = int(number_of_threads)
    if number_of_threads not in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]:
        number_of_threads = 5
        print("Invalid value for number of threads, continuing with",number_of_threads,"threads")
except:
    number_of_threads = 5
    print("Invalid value for number of threads, continuing with",number_of_threads,"threads")

try:
    max_rate_limit = float(max_rate_limit)
    if not 0.2 <= max_rate_limit <= 0.6:
        max_rate_limit = 0.4
        print("Invalid value for max rate limit to consume, continuing with",max_rate_limit)
except:
    max_rate_limit = 0.4
    print("Invalid value for max rate limit to consume, continuing with",max_rate_limit)

if not id_to_continue_from:
    batch_list.to_csv(output_file_path, index=False)
else:
    if id_to_continue_from in input_user_list.values:
        input_user_list = input_user_list[input_user_list[input_user_list == id_to_continue_from].index[0]+1:]
    else:
        print(id_to_continue_from, "not found, appending current output to", output_file_path)
    if output_file_path == "user_factors_"+append_time+".csv":
        batch_list.to_csv(output_file_path, index=False)

execution_start = datetime.datetime.now()
print("Script execution started at:", execution_start)
logging.info('Script execution started at: %s',execution_start)
logging.info('Okta tenant base url: %s',base_url)
logging.info('Okta api token: %s',api_token)
logging.info('Input filepath: %s',input_file_path)
logging.info('Id to continue from: %s',id_to_continue_from)
logging.info('Output filepath: %s',output_file_path)
logging.info('Number of threads: %s',number_of_threads)
            
#queue not iterable hence search in series, modify and then load in queue
input_user_list_size = len(input_user_list)
shared_queue = queue.Queue(maxsize=0)
[shared_queue.put(id) for id in input_user_list]
input_user_list = None

thread_lock = threading.Lock()

urllib3.disable_warnings() #to suppress unverified https request warning

def update_progress(progress, message):
    print('\r[ {0}{1} ] {2}% {3}'.format('#' * int(progress/2), ' ' * int(50 - progress/2), progress, message),end="")

def worker_thread():
    limit = 0
    remain = 0
    reset = 0
    batch_list = pandas.DataFrame(columns = selected_columns)
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
            uid = '0'*int(8-len(str(uid)))+str(uid) if int(8-len(str(uid))) > 0 else str(uid)
            current_uid = uid
            uid="\""+uid+"\""
            endpoint = "{}/api/v1/users?search=profile.AD_LDAP_Mapper+eq+{}+or+profile.idx_Uid+eq+{}+or+profile.AD_SAMAccountName+eq+{}".format(base_url,uid,uid,uid)
            response = requests.get(endpoint, headers=headers, verify = False)
            limit = response.headers['x-rate-limit-limit']
            remain = response.headers['x-rate-limit-remaining']
            reset = response.headers['x-rate-limit-reset']
            if response.status_code==200:
                if(len(response.json()) != 0):
                    current_user_details = pandas.json_normalize(response.json(), errors='ignore').reindex(columns=selected_columns).fillna('')
                    current_user_details.at[0, 'uid'] = current_uid
                    current_user_details.at[0, 'result'] = "MFA Found"
                    endpoint = "{}/api/v1/users/{}/factors".format(base_url,current_user_details["id"].values[0])
                    response = requests.get(endpoint, headers=headers, verify = False)
                    if response.status_code==200:
                        if(len(response.json()) == 0):
                            current_user_details.at[0, 'result'] = "MFA not found"
                    else:
                        current_user_details.at[0, 'result'] = "Unexpected response from factors API"

                    batch_list = pandas.concat([batch_list,current_user_details], ignore_index=True)

                else:
                    current_user_details = pandas.DataFrame(columns = selected_columns)
                    current_user_details.at[0, 'uid'] = current_uid
                    current_user_details.at[0, 'result'] = "User not found"
                    batch_list = pandas.concat([batch_list, current_user_details], ignore_index=True)
            else:
                current_user_details = pandas.DataFrame(columns = selected_columns)
                current_user_details.at[0, 'uid'] = current_uid
                current_user_details.at[0, 'result'] = "Unexpected response from users API"
                batch_list = pandas.concat([batch_list, current_user_details], ignore_index=True)

            if len(batch_list.index) >= 50:
                thread_lock.acquire()
                try:
                    batch_list.to_csv(output_file_path, index=False, header=False, mode='a')
                except Exception as e:
                    logging.warning('Error while saving to file: %s',e)
                    logging.warning('DF - %s',batch_list.to_string())
                    logging.warning('List of failed input ids: %s',batch_list['uid'].tolist())
                    print("Error while saving to file: ",e)
                    print(batch_list.to_string())
                    print(batch_list['uid'].tolist())
                thread_lock.release()
                batch_list = batch_list[0:0]
                update_progress( int((total_processed / input_user_list_size)*100) , "Users written to file : "+str(total_processed)+"              " )
            else:
                update_progress( int((total_processed / input_user_list_size)*100) , "Users processed currently : "+str(total_processed)+"          " )

            if int(limit)-int(remain) > int(limit)*max_rate_limit:
                update_progress( int((total_processed / input_user_list_size)*100) , "Waiting for rate limit: "+
                                str( abs( int(reset) - int(time.time()) ) )+"s, "+
                                str(total_processed)+" done " )
                time.sleep(abs( int(reset) - int(time.time()) ) )

            shared_queue.task_done()

        except Exception as e:
            current_user_details = pandas.DataFrame(columns = selected_columns)
            current_user_details.at[0, 'uid'] = current_uid
            current_user_details.at[0, 'result'] = "Runtime Error"
            batch_list = pandas.concat([batch_list, current_user_details], ignore_index=True)
            shared_queue.task_done()
            print("Runtime Error: ",e)

    if len(batch_list.index) != 0:
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
logging.info('Script execution completed at: %s',execution_end)
logging.info('Total time taken for script execution: %s',(execution_end - execution_start))