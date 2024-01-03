# Instructions

All scripts are multithreaded and the python process may have to terminated using task manager to stop the script while its running

As the script is multithreaded, please check the output file (incomplete) to get the total number of ids processed and accordingly enter the id at that position from the input file if you wish to continue from that point

DO NOT - enter the last id from the incompelte output file as this may not be necessarily the last processed record (due to multithreading)

EG - if the output file has 125 records processed, enter the 125th id as the 'user id to continue from' optional parameter 

Please check if correct attribute is used in the API call (eg either profile.login or profile.email depending on input file)

For scripts where only single column is input (eg get MFA status) ensure that the input file has the either the GPID or username as the first column with any header

For scripts where multiple columns are inputs (enrolling SMS and Voice factors) the column can be in any order and index but ensure that the 
GPID/username column has harder name - 'uid', 
sms factor to enroll column has header name - 'sms', 
voice factor to enroll has header name  - 'voice'
