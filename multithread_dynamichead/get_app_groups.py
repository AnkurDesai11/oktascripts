import datetime, json, logging, pandas, queue, requests, time, threading, urllib3

append_time = datetime.datetime.now().strftime("%d%b%Y_%H%M%S")
logging.basicConfig(filename=str("log"+append_time+".out"), format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

base_url = input("Enter Okta tenant base url: ")
api_token = input("Enter Okta api token: ")
output_file_path = input("OPTIONAL - Enter filepath to save output (absolute); DEFAULT - 'user_factors_[timestamp].csv' in the same location as script: ") or "user_factors_"+append_time+".csv"
fetch_limit = 200

if(not(base_url.split("/", 1)[0] == "https:" or base_url.split("/", 1)[0] == "http:" )):
    base_url = "https://"+base_url

selected_columns = ["id", "name", "label", "_links.groups.href","groups_ids", "group_names"]
all_app_details = pandas.DataFrame(columns = selected_columns)
headers = {'accept': 'application/json','content-type': 'application/json','authorization' : 'SSWS {}'.format(api_token)}
endpoint = "{}/api/v1/apps?limit={}".format(base_url,fetch_limit)
response_length = 0
response = []

execution_start = datetime.datetime.now()
print("Script execution started at:", execution_start)
logging.info('Script execution started at: %s',execution_start)
logging.info('Okta tenant base url: %s',base_url)
logging.info('Okta api token: %s',api_token)
logging.info('Output filepath: %s',output_file_path)
logging.info('Values fetched per call: %s',fetch_limit)

urllib3.disable_warnings() #to suppress unverified https request warning

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

def worker_thread():
    limit = 0
    remain = 0
    reset = 0
    batch_list = pandas.DataFrame(columns = selected_columns)
    while not shared_queue.empty():
        try:
            current_app_details = shared_queue.get()
            global total_processed
            thread_lock.acquire()
            total_processed += 1
            thread_lock.release()