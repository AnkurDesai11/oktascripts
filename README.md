# Okta Automation Scripts

Python scripts to perform bulk actions using okta apis.

Some of the actions that can be performed automatically using these scripts are as follows
* Create multiple activated users
* Get user details for users in the Okta tenant
* Get other specific user details (not returned via the /users/{{userId}} such as groups assigned, enrolled factors
* Get Status of user's enrolled factors and also enroll users in factors like voice and sms
* Update specific attributes in users profile

To execute, run any of the scripts in a Python IDE, or CLI with python installed. All scripts will mandatorily require the base url of the okta tenant and a active API token for that tenant.
Most scripts have many other optional input parameters and their discriptions can be found when prompted for input during execution. Optional parameters can be skipped with enter keypress.
