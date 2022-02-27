# DMSTaskCopyTool

## Overview
Database Migration Service (DMS) allows on premise databases to be migrated into AWS cloud seamlessly. However, when working with different staging and testing environments (like Dev/Staging/UAT/Production), it becomes difficult to manage DMS task promotion across environments. Manual copy of DMS tasks between the customer’s AWS accounts as they promote the solution across each testing/staging environment is a tedious and time consuming task. An automated solution is the need of the hour to reduce manual repetitive tasks and errors. In this blog post, we present a simple command line tool that will help customers to tag the DMS task resources on a source AWS account that they would like to promote to the next higher staging environment.

## Prerequisites
- Python 3.4 Version or higher version installed in your system.
- Boto3 SDK must be installed.
- At least 2 different AWS accounts with AWS IAM cross account based roles defined with sufficient privileges and access to DMS Service.
- The DMS source and target endpoints, DMS replication instances and DMS tasks tagged with a filter for promoting to the next higher staging environment (all tagged resources are assumed to be available in us-east-1 region and can be configured to any region).
- An understanding on AWS DMS migration task, Replication instance and Endpoints to connect to source and target environment. 
- A replication subnet group should be created and available on the target AWS account and provided in the configuration file for rep_subnet_grp_identifier option under rep_instances.

## Run the tool

First run the below command to setup the dependencies for the tool:

It is recommended for a virtual environment to be created.

pip install -r requirements.txt

Next, modify the dms_config.json file attributes based on the word document (DMSTaskCopy_README.docx) supplied in this repository. This word document serves to explain all the attributes required for the DMS Task Copy Tool to work as intended.

Please note that you must have the correct cross account IAM roles created in AWS source and target accounts with STS:AssumeRole permissions. Note: This is only required if you are authenticating outside of AD (Active Directory) directly with your source and target AWS accounts.

These IAM role ARN's must be defined in the dms_config.json file for the 'sts_src_role_arn' and 'sts_tgt_role_arn' attributes and the 'ad_authentication' option in the dms_config.json file must be set to 'false'. Otherwise, you must define your AD user name against 'ad_username' option in the config file and also specify the identity service exposed which can accept the AD user name and authenticate using a pre-defined role for both your source and target AWS accounts. Note: The tool expects the AD password credential to be set as an environment variable.

When you run the tool, only the mode argument is mandatory:

* EXPORT — To export the DMS tasks from Source environment based on tags defined for the tasks and endpoints.
* IMPORT — To import the DMS tasks to target environment. It has further options to cleanup the existing tasks in target environment. Use Y/N to clear the existing tasks, endpoints and instanes. 
### Note: 
The tool will only cleanup the resources created by it in an earlier run with same configuration settings.

Modify the json config file for the source and target accounts. The json config file must be placed in the same directory from where the tool is run.

The following is an example of tool execution:

python DMSTaskCopy.py --mode [export/export]

(or)

python DMSTaskCopy.py -m [export/import]

The below optional arguments for source endpoint password and target endpoint password can also be passed if the user does not wish to add the passwords in the configuration file for endpoints.password attributes.

python DMSTaskCopy.py --mode [export/export] --src_ep_passwd [password] --tgt_ep_passwd [password]

(or)

python DMSTaskCopy.py -m [export/import] -src_ep_passwd [password] -tgt_ep_passwd [password]


## Output:
The outputs for the two modes of the tool will be as follows.
Export: Based on the dms_sudbir folder name provided in the json config file, the DMS task table mappings and replication task settings will be extracted as json strings and stored here. Users can manually edit the tablemappings, replication task settings files as required and invoke the tool in the import mode so that the DMS tasks will be recreated on the target AWS account with the modified settings.
Import: The tool will create the DMS Tasks, endpoints, replication instance details in target account.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
