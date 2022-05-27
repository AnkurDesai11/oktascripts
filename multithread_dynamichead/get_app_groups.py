import datetime, json, logging, pandas, queue, requests, time, threading, urllib3

append_time = datetime.datetime.now().strftime("%d%b%Y_%H%M%S")
logging.basicConfig(filename=str("log"+append_time+".out"), format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

base_url = input("Enter Okta tenant base url: ")
api_token = input("Enter Okta api token: ")
output_file_path = input("OPTIONAL - Enter filepath to save output (absolute); DEFAULT - 'all_apps_groups_[timestamp].csv' in the same location as script: ") or "all_apps_groups_"+append_time+".csv"
fetch_limit = 200
number_of_threads = input("OPTIONAL - Enter number of threads to run; DEFAULT - 5 (MIN/RECOMMENDED/MAX - 1/<10/15): ") or 5
max_rate_limit = input("OPTIONAL - Enter max rate limit to consume; DEFAULT - 0.4 (MIN/MAX - 0.2/0.6): ") or 0.4

if(not(base_url.split("/", 1)[0] == "https:" or base_url.split("/", 1)[0] == "http:" )):
    base_url = "https://"+base_url

selected_columns = ["id", "name", "label", "_links.groups.href","group_ids", "group_names"]
all_app_details = pandas.DataFrame(columns = selected_columns)
headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(api_token)}
endpoint = "{}/api/v1/apps?limit={}".format(base_url,fetch_limit)
response_length = 0
response = []

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

execution_start = datetime.datetime.now()
print("Script execution started at:", execution_start)
logging.info('Script execution started at: %s',execution_start)
logging.info('Okta tenant base url: %s',base_url)
logging.info('Okta api token: %s',api_token)
logging.info('Output filepath: %s',output_file_path)
logging.info('Values fetched per call: %s',fetch_limit)

urllib3.disable_warnings() #to suppress unverified https request warning

all_app_details.to_csv(output_file_path, index=False)

################################################################
########## Fetching all apps in Okta tenant
################################################################
try:
    response = requests.get(endpoint, headers=headers, verify = False)
    if response.status_code==200:
        response_length = len(response.json())
    else:
        print("Unexpected Response from Okta:-",response.status_code," ",response.reason)
except  Exception as e:
    print("Error in calling API: "+str(e))

while response_length > 0:
    try:
        current_app_details = pandas.json_normalize(response.json(), errors='ignore').reindex(columns=selected_columns).fillna('')
        all_app_details = pandas.concat([all_app_details, current_app_details], ignore_index=True)
        if(response_length<fetch_limit):
            break
        else:
            endpoint = response.headers['link'].split(", <", 1)[1].split(">", 1)[0]
            response = requests.get(endpoint, headers=headers, verify = False)
            if response.status_code==200:
                response_length = len(response.json())
            else:
                response_length = 0
                print("Unexpected Response from Okta:-",response.status_code," ",response.reason)
    except  Exception as e:
        print("Error in processing: "+str(e))

print("Fetched", len(all_app_details),"apps")

################################################################
########## Using list of fetched apps to get their groups
################################################################
app_list_size = len(all_app_details)
shared_queue = queue.Queue(maxsize=0)
[shared_queue.put(row) for row in all_app_details.to_dict('records')]
total_processed = 0

thread_lock = threading.Lock()

def update_progress(progress, message):
    print('\r[ {0}{1} ] {2}% {3}'.format('#' * int(progress/2), ' ' * int(50 - progress/2), progress, message),end="")

def get_group_name(group_id):
    try:
        return requests.get("{}/api/v1/groups/{}".format(base_url,group_id), headers=headers, verify = False).json()["profile"]["name"]
    except Exception as e:
        logging.warning('Error while fetching group name: %s',e)
        return "Error while fetching group name"

def worker_thread():
    limit = 0
    remain = 0
    reset = 0
    batch_list = pandas.DataFrame(columns = selected_columns)
    current_app_details = {"id": '', "name": '', "label": '', "_links.groups.href": '',"group_ids": '', "group_names": ''}
    while not shared_queue.empty():
        try:
            current_app_details = shared_queue.get()
            global total_processed
            thread_lock.acquire()
            total_processed += 1
            thread_lock.release()
            response = requests.get(current_app_details["_links.groups.href"], headers=headers, verify = False)
            limit = response.headers['x-rate-limit-limit']
            remain = response.headers['x-rate-limit-remaining']
            reset = response.headers['x-rate-limit-reset']
            if response.status_code==200:
                if(len(response.json()) != 0):
                    current_app_group_ids = [ obj['id'] for obj in response.json() ]
                    current_app_details["group_ids"] = ','.join(current_app_group_ids)
                    current_app_details["group_names"] = ','.join([ get_group_name(id) for id in current_app_group_ids ])
                else:
                    current_app_details["group_ids"] = "No groups assigned"
                    current_app_details["group_names"] = "No groups assigned"
            else:
                current_app_details["group_ids"] = "Unexpected response from groups API"
                current_app_details["group_names"] = "Unexpected response from groups API"
            
            batch_list = pandas.concat([batch_list,pandas.DataFrame([current_app_details])], ignore_index=True)

            if len(batch_list.index) >= 50:
                thread_lock.acquire()
                try:
                    batch_list.to_csv(output_file_path, index=False, header=False, mode='a')
                except Exception as e:
                    logging.warning('Error while saving to file: %s',e)
                    logging.warning('DF - %s',batch_list.to_string())
                    logging.warning('List of failed ids: %s',batch_list['id'].tolist())
                    print("Error while saving to file: ",e)
                    print(batch_list.to_string())
                    print(batch_list['id'].tolist())
                thread_lock.release()
                batch_list = batch_list[0:0]
                update_progress( int((total_processed / app_list_size)*100) , "Apps written to file : "+str(total_processed)+"              " )
            else:
                update_progress( int((total_processed / app_list_size)*100) , "Apps processed currently : "+str(total_processed)+"          " )

            if int(limit)-int(remain) > int(limit)*max_rate_limit:
                update_progress( int((total_processed / app_list_size)*100) , "Waiting for rate limit: "+
                                str( abs( int(reset) - int(time.time()) ) )+"s, "+
                                str(total_processed)+" done " )
                time.sleep(abs( int(reset) - int(time.time()) ) )

            shared_queue.task_done()

        except Exception as e:
            logging.warning('Runtime Error: %s',e)
            current_app_details["group_ids"] = "Runtime Error"
            current_app_details["group_names"] = "Runtime Error"
            batch_list = pandas.concat([batch_list,pandas.DataFrame([current_app_details])], ignore_index=True)
            shared_queue.task_done()
        
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