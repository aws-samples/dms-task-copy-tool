#!/usr/bin/env python
#
#   DMSTaskCopy.py
#
#   Description - A standalone script that attempts to copy the DMS replication tasks from a source account
#   to the destination account of customers. This will be useful when the DMS tasks have to be re-created
#   in the higher staging environments before production deployments.
#
# /*
#  * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  * SPDX-License-Identifier: MIT-0
#  *
#  * Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  * software and associated documentation files (the "Software"), to deal in the Software
#  * without restriction, including without limitation the rights to use, copy, modify,
#  * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  * permit persons to whom the Software is furnished to do so.
#  *
#  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#  */

import os
import sys
import boto3
from botocore.exceptions import ClientError
import json
import requests
import logging
import argparse
import time
import datetime
import inspect
import urllib3
import concurrent.futures
import tablemappings
import replicationtasksettings
import cleancreatedresources
import threading
from threading import Thread, Lock
from itertools import chain
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from boto3.session import Session
import datawrapper

lock = Lock()

# This method allows the tool to assume a role that provides only the required DMS
# service access to perform its work.
def get_credentials_for_role_arn(arn, session_name):
    """aws sts assume-role --role-arn arn:aws:iam::00000000000000:role/example-role --role-session-name example-role"""
    assume_role_credentials = dict()
    client = boto3.client('sts')
    account_id = client.get_caller_identity()["Account"]
    print("Account ID = ", account_id)

    assume_role_response = client.assume_role(RoleArn=arn, RoleSessionName=session_name)
    assume_role_credentials['awsAccessKey'] = assume_role_response['Credentials']['AccessKeyId']
    assume_role_credentials['awsSecretKey'] = assume_role_response['Credentials']['SecretAccessKey']
    assume_role_credentials['awsSessionToken'] = assume_role_response['Credentials']['SessionToken']
    print("STS Response = ", assume_role_response)
    session = Session(aws_access_key_id=assume_role_response['Credentials']['AccessKeyId'],
                      aws_secret_access_key=assume_role_response['Credentials']['SecretAccessKey'],
                      aws_session_token=assume_role_response['Credentials']['SessionToken'])

    client = session.client('sts')
    account_id = client.get_caller_identity()["Account"]
    print(account_id)

    return assume_role_credentials

# This is a sample method when authenticating with on premise AD where the
# service is exposed as a REST API for internal employees.
def get_credentials_for_role(user_name, account_id, role_name, password, identity_service_url, cert_verify_flag):
    # Create a new request
    headers = {"Content-Type": "application/json", "Accept-version": "1"}
    data = {
        "username": "{0}".format(user_name),
        "accountId": "{0}".format(account_id),
        "roleName": "{0}".format(role_name),
        "password": "{0}".format(password),
        "duration": 7200
    }
    # logger.info("Data={0}".format(data))
    logger.info ("Trying to get credentials from identity service for user = {0}".format(user_name))
    # Note - Replace xyz with correct domain name!
    response = requests.post(identity_service_url, headers=headers, json=data, verify=cert_verify_flag)
    logger.info("Response from REST API={0}".format(response.json()))
    credentials = response.json()
    return credentials

# Gets the correct endpoint for updation
def get_endpoints_elem(endpoints_dict, count, EPArn):
    logger.info("endpoints_dict={0},count={1},endpoint_arn={2}".format(endpoints_dict,count,EPArn))
    for i in range(count):
        if endpoints_dict[i]['endpoint_arn'] == EPArn:
            logger.info("get_endpoints_elem ret value = {0}".format(i))
            return i
    return -1

# Gets the correct rep inst for updation
def get_rep_inst_elem(rep_inst_dict,count,RepInstArn):
    logger.info("rep_inst_dict={0},count={1},rep_inst_id={2}".format(rep_inst_dict,count,RepInstArn))
    for i in range(count):
        if rep_inst_dict[i]['rep_inst_arn'] == RepInstArn:
            logger.info("get_rep_inst_elem ret value = {0}".format(i))
            return i
    return -1

# Gets the correct endpoint for updation based on id
def get_ep_elem_from_id(ep_dict,count,EPInstId):
    logger.info("ep_dict={0},count={1},ep_inst_id={2}".format(ep_dict,count,EPInstId))
    for i in range(count):
        if ep_dict[i]['endpoint_identifier'] == EPInstId:
            logger.info("get_ep_elem ret value = {0}".format(i))
            return i
    return -1

# Gets the correct endpoint for updation from type
def get_jdata_elem_from_type(jdata, count, endpoint_type):
    for i in range(count):
        if jdata['endpoints'][i]['endpoint_type'].upper() in endpoint_type.upper():
            logger.info("get_jdata_elem_from_type ret value = {0}".format(i))
            return i
    return -1

# Gets the correct endpoint for updation from user type
def get_ep_elem_from_user_type(ep_dict,count,EPuser, EPtype):
    logger.info("ep_dict={0},count={1},ep_type={2}, ep_user={3}".format(ep_dict,count,EPtype,EPuser))
    for i in range(count):
        logger.info("comparing the username in source account = {0} with the username defined in config file = {1}"
                        .format(ep_dict[i]['user_name'],EPuser))
        #Need to check if the approach of using user name is good enough....
        #if ep_dict[i]['endpoint_type'] in EPtype.upper() and ep_dict[i]['user_name'].lower() in EPuser.lower():
        if ep_dict[i]['endpoint_type'] in EPtype.upper():
            logger.info("get_ep_elem ret value = {0}".format(i))
            return i
    return -1

# Gets the correct rep inst for updation from id
def get_rep_inst_elem_from_id(rep_inst_dict,count,RepInstId):
    logger.info("rep_inst_dict={0},count={1},rep_inst_id={2}".format(rep_inst_dict,count,RepInstId))
    for i in range(count):
        if rep_inst_dict[i]['rep_inst_id'] == RepInstId:
            logger.info("get_rep_inst_elem ret value = {0}".format(i))
            return i
    return -1

# Updates the endpoint configurations
def update_endpoints_dict(endpoints_dict, jdata):
    jdata_ep_count = sum([len(jdata['endpoints'])])
    endpoints_dict_count = sum([len(endpoints_dict)])
    logger.info("Inside update_endpoints_dict...")
    # Redesign to apply the endpoint_tpye=source settings to all eligible AWS source account DMS endpoints
    #for i in range(jdata_ep_count):
    for i in range(endpoints_dict_count):
        index = i
        logger.info("endpoints_dict index = {0}".format(index))
        if index != -1:
            if (jdata['endpoints_separate_elem_approach']).lower() in "true":
                # jdata_index = get_jdata_elem_from_type(jdata, jdata_ep_count,
                #                                        endpoints_dict[index]['endpoint_type'] )
                EPInstId = jdata['endpoints'][i]['endpoint_identifier']
                jdata_index = get_ep_elem_from_id(endpoints_dict, endpoints_dict_count, EPInstId)
            else:
                jdata_index = get_jdata_elem_from_type(jdata, jdata_ep_count,
                                                       endpoints_dict[index]['endpoint_type'])
            if jdata_index == -1:
                logger.fatal("ERROR: jdata endpoint index for endpoint_type [{1}] is invalid.".format(endpoints_dict[index]['endpoint_type']))
                sys.exit(1)
            logger.info("jdata_ep index = {0}".format(jdata_index))
            if jdata['endpoints'][jdata_index]['endpoint_arn'] != "":
                endpoints_dict[index]['endpoint_new_arn'] = jdata['endpoints'][jdata_index]['endpoint_arn']
            if jdata['endpoints'][jdata_index]['engine_name'] != "":
                endpoints_dict[index]['engine_name'] = jdata['endpoints'][jdata_index]['engine_name']
            if jdata['endpoints'][jdata_index]['user_name'] != "":
                endpoints_dict[index]['user_name'] = jdata['endpoints'][jdata_index]['user_name']
            if jdata['endpoints'][jdata_index]['password'] != "":
                endpoints_dict[index]['password'] = jdata['endpoints'][jdata_index]['password']
            if jdata['endpoints'][jdata_index]['server_name'] != "":
                endpoints_dict[index]['server_name'] = jdata['endpoints'][jdata_index]['server_name']
            if jdata['endpoints'][jdata_index]['database_port'] != "":
                endpoints_dict[index]['database_port'] = jdata['endpoints'][jdata_index]['database_port']
            if jdata['endpoints'][jdata_index]['database_name'] != "":
                endpoints_dict[index]['database_name'] = jdata['endpoints'][jdata_index]['database_name']
            if jdata['endpoints'][jdata_index]['extra_conn_attr'] != "":
                endpoints_dict[index]['extra_conn_attr'] = jdata['endpoints'][jdata_index]['extra_conn_attr']
            if jdata['endpoints'][jdata_index]['endpoint_type'] != "":
                endpoints_dict[index]['endpoint_type'] = jdata['endpoints'][jdata_index]['endpoint_type']
            endpoints_dict[index]['endpoint_identifier_new'] = jdata['endpoints'][jdata_index]['endpoint_identifier_new']
            endpoints_dict[index]['endpoint_identifier_special_chars'] = jdata['endpoints'][jdata_index]['endpoint_identifier_special_chars']
            ###endpoints_dict[index]['endpoint_arn'] = ""
        else:
            logger.fatal("ERROR: endpoint_identifier in config file does not match source endpoints")
            sys.exit(1)

# Updates the replication instances 
def update_rep_inst_dict(rep_inst_dict, jdata):
    #print("inside update dict fn")
    jdata_rep_inst_count = sum([len(jdata['rep_instances'])])
    rep_inst_dict_count = sum([len(rep_inst_dict)])
    #print("update+rep+inst+dict=",rep_inst_dict_count)
    for i in range(jdata_rep_inst_count):
        if jdata['rep_instances'][i]['rep_instance_arn'] == "":
            RepInstId = jdata['rep_instances'][i]['rep_instance_identifier']
            index = get_rep_inst_elem_from_id(rep_inst_dict,rep_inst_dict_count,RepInstId)
            if index != -1:
                if jdata['rep_instances'][i]['rep_instance_class'] != "":
                    rep_inst_dict[index]['rep_inst_class'] = jdata['rep_instances'][i]['rep_instance_class']
                if jdata['rep_instances'][i]['rep_engine_version'] != "":
                    rep_inst_dict[index]['rep_engine_version'] = jdata['rep_instances'][i]['rep_engine_version']
                if jdata['rep_instances'][i]['allocated_storage'] != "":
                    rep_inst_dict[index]['rep_inst_storage'] = jdata['rep_instances'][i]['allocated_storage']
                if jdata['rep_instances'][i]['availability_zone'] != "":
                    rep_inst_dict[index]['rep_inst_az'] = jdata['rep_instances'][i]['availability_zone']
                if jdata['rep_instances'][i]['preferred_maint_window'] != "":
                    rep_inst_dict[index]['rep_inst_maint'] = jdata['rep_instances'][i]['preferred_maint_window']
                if jdata['rep_instances'][i]['multi_az_bool'] != "":
                    rep_inst_dict[index]['rep_inst_maz'] = jdata['rep_instances'][i]['multi_az_bool']
                if jdata['rep_instances'][i]['vpc_security_group_ids'][0] != "":
                    #print("setting rep_inst_sg with empty array!")
                    rep_inst_dict[index]['rep_inst_sg'] = jdata['rep_instances'][i]['vpc_security_group_ids']
                if jdata['rep_instances'][i]['rep_subnet_grp_identifier'] != "":
                    rep_inst_dict[index]['rep_inst_vpc'] = jdata['rep_instances'][i]['rep_subnet_grp_identifier']
                if jdata['rep_instances'][i]['publicly_accessible_bool'] != "":
                    rep_inst_dict[index]['rep_inst_pub'] = jdata['rep_instances'][i]['publicly_accessible_bool']
                rep_inst_dict[index]['rep_instance_identifier_new'] = jdata['rep_instances'][i]['rep_instance_identifier_new']
            else:
                logger.error("Exception: replication instance identifier in config file does not match source replication instance identifier")
                index = 0
                if jdata['rep_instances'][i]['rep_instance_class'] != "":
                    rep_inst_dict[index]['rep_inst_class'] = jdata['rep_instances'][i]['rep_instance_class']
                if jdata['rep_instances'][i]['rep_engine_version'] != "":
                    rep_inst_dict[index]['rep_engine_version'] = jdata['rep_instances'][i]['rep_engine_version']
                if jdata['rep_instances'][i]['allocated_storage'] != "":
                    rep_inst_dict[index]['rep_inst_storage'] = jdata['rep_instances'][i]['allocated_storage']
                if jdata['rep_instances'][i]['availability_zone'] != "":
                    rep_inst_dict[index]['rep_inst_az'] = jdata['rep_instances'][i]['availability_zone']
                if jdata['rep_instances'][i]['preferred_maint_window'] != "":
                    rep_inst_dict[index]['rep_inst_maint'] = jdata['rep_instances'][i]['preferred_maint_window']
                if jdata['rep_instances'][i]['multi_az_bool'] != "":
                    rep_inst_dict[index]['rep_inst_maz'] = jdata['rep_instances'][i]['multi_az_bool']
                if jdata['rep_instances'][i]['vpc_security_group_ids'][0] != "":
                    #print("setting rep_inst_sg with empty array!")
                    rep_inst_dict[index]['rep_inst_sg'] = jdata['rep_instances'][i]['vpc_security_group_ids']
                if jdata['rep_instances'][i]['rep_subnet_grp_identifier'] != "":
                    rep_inst_dict[index]['rep_inst_vpc'] = jdata['rep_instances'][i]['rep_subnet_grp_identifier']
                if jdata['rep_instances'][i]['publicly_accessible_bool'] != "":
                    rep_inst_dict[index]['rep_inst_pub'] = jdata['rep_instances'][i]['publicly_accessible_bool']
                rep_inst_dict[index]['rep_instance_identifier_new'] = jdata['rep_instances'][i]['rep_instance_identifier_new']
        else:
            RepInstId = jdata['rep_instances'][i]['rep_instance_identifier']
            index = get_rep_inst_elem_from_id(rep_inst_dict, rep_inst_dict_count, RepInstId)
            if index != -1:
                #index = rep_inst_dict_count + 1
                rep_inst_dict[index]['rep_inst_new_arn'] = jdata['rep_instances'][i]['rep_instance_arn']
            else:
                logger.fatal("ERROR: replication instance identifier in config file does not match source replication instance identifier")

# Creates the endpoints
def createEndpoints(endpoints_dict, inst):
    if endpoints_dict[inst]['endpoint_new_arn'] == "":
        retryFlag = True
        retryCount = 1
        while retryFlag and retryCount > 0:
            try:
                endpoint_identifier = ""
                if endpoints_dict[inst]['endpoint_identifier_new'] != "":
                    endpoint_identifier = endpoints_dict[inst]['endpoint_identifier_new']
                else:
                    if endpoints_dict[inst]['database_name'] == "" or endpoints_dict[inst]['database_name'] == None:
                        endpoint_identifier = endpoints_dict[inst]['endpoint_identifier']
                    else:
                        endpoint_identifier = (endpoints_dict[inst]['database_name']) + "-" + jdata['dest_environment'] + str(inst)
                        if endpoints_dict[inst]['endpoint_identifier_special_chars'] != "":
                            special_chars_count = len(endpoints_dict[inst]['endpoint_identifier_special_chars'])
                            for elem in range(special_chars_count):
                                special_char = endpoints_dict[inst]['endpoint_identifier_special_chars'][elem]
                                endpoint_identifier = endpoint_identifier.replace(special_char, "-")
                logger.info("Trying to create endpoint...Endpoint Identifier = {0}".format(endpoint_identifier))
                #logger.info("Trying to create endpoint...{0}".format(endpoints_dict[inst]))
                if endpoints_dict[inst]['endpoint_sqlserver_settings'] != None and endpoints_dict[inst]['endpoint_sqlserver_settings'] != "":
                    #print(endpoints_dict[inst]['endpoint_sqlserver_settings'])
                    lock.acquire()
                    dms2response = dms2.create_endpoint(
                        EndpointIdentifier=endpoint_identifier,
                        EndpointType=endpoints_dict[inst]['endpoint_type'].lower(),
                        EngineName=endpoints_dict[inst]['engine_name'],
                        Username=endpoints_dict[inst]['user_name'],
                        Password=endpoints_dict[inst]['password'],
                        ServerName=endpoints_dict[inst]['server_name'],
                        Port=int(endpoints_dict[inst]['database_port']),
                        DatabaseName=endpoints_dict[inst]['database_name'],
                        MicrosoftSQLServerSettings=endpoints_dict[inst]['endpoint_sqlserver_settings'],
                        ExtraConnectionAttributes=endpoints_dict[inst]['extra_conn_attr'],
                        Tags=[{'Key': jdata['rep_task_tag_filters'][0]['Key'], 'Value': jdata['dest_environment']}]
                    )
                    lock.release()
                    logger.info("dms2 endpoint created = {0}".format(dms2response))
                elif endpoints_dict[inst]['endpoint_postgres_settings'] != None and endpoints_dict[inst]['endpoint_postgres_settings'] != "":
                    #logger.info("elif endpoints_dict = {0}".format(endpoints_dict[inst]['endpoint_postgres_settings']))
                    lock.acquire()
                    dms2response = dms2.create_endpoint(
                        EndpointIdentifier=endpoint_identifier,
                        EndpointType=endpoints_dict[inst]['endpoint_type'].lower(),
                        EngineName=endpoints_dict[inst]['engine_name'],
                        Username=endpoints_dict[inst]['user_name'],
                        Password=endpoints_dict[inst]['password'],
                        ServerName=endpoints_dict[inst]['server_name'],
                        Port=int(endpoints_dict[inst]['database_port']),
                        DatabaseName=endpoints_dict[inst]['database_name'],
                        PostgreSQLSettings=endpoints_dict[inst]['endpoint_postgres_settings'],
                        ExtraConnectionAttributes=endpoints_dict[inst]['extra_conn_attr'],
                        Tags=[{'Key': jdata['rep_task_tag_filters'][0]['Key'], 'Value': jdata['dest_environment']}]
                    )
                    lock.release()
                    logger.info("dms2 endpoint created = {0}".format(dms2response))
                #logger.info("endpoint_sqlserver_settings={0}".format(endpoints_dict[inst]['endpoint_sqlserver_settings']))
                #logger.info("endpoint_postgres_settings={0}".format(endpoints_dict[inst]['endpoint_postgres_settings']))

                retryFlag = False
                retryCount = retryCount - 1
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceAlreadyExistsFault':
                    if str(e).find("already in use"):
                        endpointARN = str(e).split()[-1]
                        logger.error("Exception Occurred When Creating Endpoint:{0}".format(str(e)))
                        sys.exit(1)
                    else:
                        logger.info("ERROR:{0}".format(str(e)))
                        logger.info("Exiting...")
                        sys.exit(1)
                else:
                    logger.info("ERROR:{0}".format(str(e)))
                    logger.info("Exiting...")
                    sys.exit(1)
            except Exception as e:
                logger.error("ERROR:{0}".format(str(e)))
                logger.info("Exiting...")
                sys.exit(1)
        lock.acquire()
        endpoints_dict[inst]['endpoint_new_arn'] = dms2response['Endpoint']['EndpointArn']
        lock.release()
    else:
        lock.acquire()
        endpoints_dict[inst]['endpoint_new_arn'] = jdata['endpoints'][inst]['endpoint_arn']
        lock.release()

# Creates the replication instances
def createRepInstances(rep_inst_dict, inst):
    try:
        sglist = []
        repl_inst_identifier = ""
        if rep_inst_dict[inst]['rep_inst_new_arn'] == "":
            sglist.append(rep_inst_dict[inst]['rep_inst_sg'])
            if rep_inst_dict[inst]['rep_instance_identifier_new'] != "":
                repl_inst_identifier = rep_inst_dict[inst]['rep_instance_identifier_new']
            else:
                repl_inst_identifier = rep_inst_dict[inst]['rep_inst_id']
            dms2.rep_response = dms2.create_replication_instance(
                ReplicationInstanceIdentifier=repl_inst_identifier,
                AllocatedStorage=int(rep_inst_dict[inst]['rep_inst_storage']),
                ReplicationInstanceClass=rep_inst_dict[inst]['rep_inst_class'],
                EngineVersion=rep_inst_dict[inst]['rep_engine_version'],
                VpcSecurityGroupIds=sglist,
                AvailabilityZone=rep_inst_dict[inst]['rep_inst_az'],
                ReplicationSubnetGroupIdentifier=rep_inst_dict[inst]['rep_inst_vpc'],
                PreferredMaintenanceWindow=rep_inst_dict[inst]['rep_inst_maint'],
                MultiAZ=rep_inst_dict[inst]['rep_inst_maz'],
                PubliclyAccessible=rep_inst_dict[inst]['rep_inst_pub'],
                Tags = [{'Key': jdata['rep_task_tag_filters'][0]['Key'], 'Value': jdata['dest_environment']}]
            )
            logger.info("Replication Instance Response={0}".format(dms2.rep_response))
            logger.info("Replication Instance ARN={0}".format(
                dms2.rep_response['ReplicationInstance']['ReplicationInstanceArn']))
            lock.acquire()
            rep_inst_dict[inst]['rep_inst_new_arn'] = dms2.rep_response['ReplicationInstance']['ReplicationInstanceArn']
            lock.release()
            logger.info("Sleeping for 1 minute to allow the replication instance to be created...Please wait...")
            time.sleep(60)
        else:
            logger.info("Not creating new instance as ARN already exists for instance index = {0}".format(str(inst)))
            lock.acquire()
            if rep_inst_dict[inst]['rep_inst_new_arn'] == "":
                rep_inst_dict[inst]['rep_inst_new_arn'] = rep_inst_dict[inst]['rep_inst_arn']
            lock.release()
    except Exception as e:
        logger.fatal("ERROR: {0}".format(str(e)))
        os._exit(1)

# Creates the replication tasks
def createRepTasks(data, x, options):
    # logger.info(data['ReplicationTasks'][x]['ReplicationTaskIdentifier'])
    RepTaskIdentifier = data['ReplicationTasks'][x]['ReplicationTaskIdentifier']
    RepTaskInstArn = data['ReplicationTasks'][x]['ReplicationInstanceArn']
    logger.info("Creating Replication Task ID ={0}".format(RepTaskIdentifier))

    if options["mode"] in "normal":
        TableMapping_content = data['ReplicationTasks'][x]['TableMappings']
        ReplicationTaskSettings_content = data['ReplicationTasks'][x]['ReplicationTaskSettings']
    elif options["mode"] in "import":
        tablmappings = tablemappings.TableMappings(RepTaskIdentifier, options["mode"], logger, dir_name)
        TableMapping_content = tablmappings.readTableMappingsFromFile()
        logger.info("readTableMappingsFromFile={0}".format(TableMapping_content))
        if TableMapping_content is None:
            raise Exception("Import invoked without Export being called.")

        replTaskSettings = replicationtasksettings.ReplicationTaskSettings(RepTaskIdentifier, options["mode"],
                                                                           logger, dir_name)
        ReplicationTaskSettings_content = replTaskSettings.readReplicationTaskSettingsFromFile()
        logger.info("readReplicationTaskSettingsFromFile={0}".format(ReplicationTaskSettings_content))
        if ReplicationTaskSettings_content is None:
            raise Exception("Import invoked without Export being called.")

    # To avoid this error:An error occurred (InvalidParameterValueException) when calling the CreateReplicationTask operation:
    # Task Settings CloudWatchLogGroup or CloudWatchLogStream cannot be set on create.
    # "CloudWatchLogGroup": null,
    # "CloudWatchLogStream": null
    ReplicationTaskSettings_content = json.loads(ReplicationTaskSettings_content)
    if ReplicationTaskSettings_content['Logging']['CloudWatchLogGroup'] != 'null' or '' or None:
        cloudWatchLogGroup = ReplicationTaskSettings_content['Logging']['CloudWatchLogGroup']
        ReplicationTaskSettings_content['Logging']['CloudWatchLogGroup'] = None
    if ReplicationTaskSettings_content['Logging']['CloudWatchLogStream'] != 'null' or '' or None:
        cloudWatchLogStream = ReplicationTaskSettings_content['Logging']['CloudWatchLogStream']
        ReplicationTaskSettings_content['Logging']['CloudWatchLogStream'] = None

    # logger.info(ReplicationTaskSettings_content['Logging']['CloudWatchLogGroup'])
    # logger.info(ReplicationTaskSettings_content['Logging']['CloudWatchLogStream'])

    MigrationType = data['ReplicationTasks'][x]['MigrationType']

    SourceEndpointArn = data['ReplicationTasks'][x]['SourceEndpointArn']
    TargetEndpointArn = data['ReplicationTasks'][x]['TargetEndpointArn']

    # Allocation of source endpoint to rep tasks
    k = get_endpoints_elem(endpoints_dict, ep_count, SourceEndpointArn)
    if(k==-1):
        logger.error("Replication Task {0} does not have a tagged source endpoint arn {1}".format(RepTaskIdentifier,SourceEndpointArn ))    
    SourceEndpointArn = endpoints_dict[k]['endpoint_new_arn']

    # Allocation of source endpoint to rep tasks
    l = get_endpoints_elem(endpoints_dict, ep_count, TargetEndpointArn)
    if(l==-1):
        logger.error("Replication Task {0} does not have a tagged target endpoint arn {1}".format(RepTaskIdentifier,TargetEndpointArn ))    
    TargetEndpointArn = endpoints_dict[l]['endpoint_new_arn']

    # Allocation of rep instance to rep tasks
    j = get_rep_inst_elem(rep_inst_dict, rep_inst_count, RepTaskInstArn)
    ReplicationInstanceArn = rep_inst_dict[j]['rep_inst_new_arn']

    retryFlag = True
    retryCount = 20
    logger.info("Outside While loop for replication task creation, retryCount = {0}".format(retryCount))
    ReplicationTaskSettings_content = json.dumps(ReplicationTaskSettings_content)
    while retryFlag and retryCount > 0:
        try:
            # logger.info("Trying to create the replication task - {0} in destination account...RetryCount={1}".
            #             format(RepTaskIdentifier,retryCount))
            logger.info("Trying to create replication task [{1}] = {0}".format(RepTaskIdentifier, x))
            rep_task_response = dms2.create_replication_task(
                ReplicationTaskIdentifier=RepTaskIdentifier,
                SourceEndpointArn=SourceEndpointArn,
                TargetEndpointArn=TargetEndpointArn,
                ReplicationInstanceArn=ReplicationInstanceArn,
                MigrationType='full-load',
                TableMappings=TableMapping_content,
                ReplicationTaskSettings=ReplicationTaskSettings_content,
                Tags=[{'Key': jdata['rep_task_tag_filters'][0]['Key'], 'Value': jdata['dest_environment']}]
            )
            logger.info("Replication Task Creation Response = {0}".format(rep_task_response))
            logger.info(
                "Sleeping for 60 seconds to retry replication task - {0} creation...".format(RepTaskIdentifier))
            time.sleep(60)
            retryFlag = False
            retryCount = retryCount - 1
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidResourceStateFault':
                if str(e).find("is not active"):
                    logger.info("Exception Occurred:{0}".format(str(e)))
                    logger.info("Sleeping for 60 seconds to retry replication task - {0} creation...".format(
                        RepTaskIdentifier))
                    time.sleep(60)
                    retryFlag = True
                    logger.info("Trying to re-create the replication task. Retry Count = {0}".format(retryCount))
                    retryCount = retryCount - 1
                else:
                    logger.error("ERROR:{0}".format(str(e)))
                    logger.info("Exiting...")
                    sys.exit(1)
            else:
                logger.error("ERROR:{0}".format(str(e)))
                logger.info("Exiting...")
                sys.exit(1)
        except Exception as e:
            logger.error("ERROR:{0}".format(str(e)))
            logger.info("Exiting...")
            sys.exit(1)
    if retryCount == 0:
        logger.info("Max number of 10 retries exceeded...Exiting!")
        sys.exit(1)
    else:
        logger.info("Replication task created = {0}".format(rep_task_response))
        # Replace the cloudWatch settings in ReplicationTaskSettings before writing to file...
        ReplicationTaskSettings_content = json.loads(ReplicationTaskSettings_content)
        if cloudWatchLogGroup != 'null' or '' or None:
            ReplicationTaskSettings_content['Logging']['CloudWatchLogGroup'] = cloudWatchLogGroup
            cloudWatchLogGroup = 'null'
        if cloudWatchLogStream != 'null' or '' or None:
            ReplicationTaskSettings_content['Logging']['CloudWatchLogStream'] = cloudWatchLogStream
            cloudWatchLogStream = 'null'

        ReplicationTaskSettings_content = json.dumps(ReplicationTaskSettings_content)

        choice = ""
        if jdata['promt_start_dms_tasks'].lower() in "true":
            choice = input("Do you want to start the replication task - {0}, (y/n)?".format(
                rep_task_response['ReplicationTask']['ReplicationTaskIdentifier']))
        elif jdata['auto_start_dms_tasks_on_creation'].lower() in "true":
            choice = "y"
        if choice.lower() == "y":
            # Start the replication task created!
            try:
                logger.info("Waiting for replication task = [{0}] to be ready...".format(
                    rep_task_response['ReplicationTask']['ReplicationTaskIdentifier']))
                waiter = dms2.get_waiter('replication_task_ready')
                waiter.wait(
                    Filters=[
                        {
                            'Name': 'replication-task-arn',
                            'Values': [
                                rep_task_response['ReplicationTask']['ReplicationTaskArn']
                            ]
                        },
                    ]
                )

                logger.info("Trying to start replication task = [{0}] to start...".format(
                    rep_task_response['ReplicationTask']['ReplicationTaskIdentifier']))
                rep_task_start_response = dms2.start_replication_task(
                    ReplicationTaskArn=rep_task_response['ReplicationTask']['ReplicationTaskArn'],
                    StartReplicationTaskType='start-replication',
                )

            except Exception as starte:
                logger.error("Error when starting created replication task: {0} is {1}".format(
                    rep_task_response['ReplicationTask']['ReplicationTaskIdentifier'], str(starte)))
                logger.info("Exiting...")
                return
            logger.info("Replication Task Started = ", rep_task_start_response)

# The main method of control for the tool...
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DMSTaskCopy')
    parser.add_argument("--mode", "-m", required=True, default="normal",
                        dest="mode", help="export (save tablemapping to file) or import (read tablemapping from file) mode", metavar="STRING")
    parser.add_argument("--src_ep_passwd", "-src_ep_passwd", required=False, default="normal",
                        dest="src_ep_passwd", help="src endpoint password", metavar="STRING")
    parser.add_argument("--tgt_ep_passwd", "-tgt_ep_passwd", required=False, default="normal",
                        dest="tgt_ep_passwd", help="tgt_ep_passwd", metavar="STRING")
    parser.add_argument("--max_workers", "-mw", required=False, default=10,
                        dest="max_workers", help="No of threads", metavar="INTEGER")
    options = vars(parser.parse_args())
    conf_file = "dms_config.json"

    current_time = datetime.datetime.now()
    dateMY = "{0}{1}{2}-{3}{4}{5}".format(current_time.year,current_time.month,current_time.day,current_time.hour,current_time.minute,current_time.second)

    # create logger with 'spam_application'
    logger = logging.getLogger('DMSTaskCopyv5')
    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler("./logs/DMSTaskCopyv5-{0}.log".format(dateMY))
    fh.setLevel(logging.INFO)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Starting DMSTaskCopy Script. Logfile: [{0}] created...".format("DMSTaskCopyv2-{0}.log".format(dateMY)))
    abspath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    configfile = abspath + os.sep + conf_file

    # More replication instances might be required to balance the tasks in the dest account...
    # Can be based on source account setup...,
    # Chunks for table with lower bound and upper bound...defined on the rep tasks,
    # logging mechanism...
    with open(configfile, 'r') as rfile:
        jdata = json.load(rfile)

    # The below code snippet where PASSWORD is an environment variable
    # is required for authenticating with AD services in an on premise organizational setup
    # where AWS identity service is exposed as a REST API for employees to
    # authenticate using their AD logins. This is not required for the purpose
    # of using this tool.
    if (jdata['ad_authentication']):
        try:
            password = os.environ['PASSWORD']
        except Exception as e:
            print("ERROR: Environment Variable not Set! - ",str(e))
            sys.exit(1)

    # Connect to source account in import mode only!
    if (options["mode"] in "export"):
        if (jdata['ad_authentication']):
            logger.info("Trying to get credentials from identity service for user = {0} and account = {1}".
                    format(jdata['ad_username'], jdata['src_account_id']))
            credentials = get_credentials_for_role(jdata['ad_username'], jdata['src_account_id'], jdata['src_account_role'], 
                            password, jdata['identity_service_url'], jdata['cert_verify_flag'])
        else:
            logger.info("Trying to get credentials by using IAM assume role arn with source account [{0}]...".format(jdata['sts_src_role_arn']))
            credentials = get_credentials_for_role_arn(jdata['sts_src_role_arn'], "DMSTaskCopyToolSession")
        dms1 = boto3.client('dms', region_name=jdata['src_account_region'], aws_access_key_id=credentials['awsAccessKey'],
                            aws_secret_access_key=credentials['awsSecretKey'],
                            aws_session_token=credentials['awsSessionToken'])
        logger.info("dms1={0}".format(dms1))

        dms1resources = boto3.client('resourcegroupstaggingapi', region_name=jdata['src_account_region'], aws_access_key_id=credentials['awsAccessKey'],
                            aws_secret_access_key=credentials['awsSecretKey'],
                            aws_session_token=credentials['awsSessionToken'])
    logger.info("mode={0}".format(options["mode"]))
    logger.info("max_workers={0}".format(options["max_workers"]))
    max_workers = options["max_workers"]

    # Connect to destination account in import mode only!
    if options["mode"] in "import":
        if (jdata['ad_authentication']):
            logger.info("Trying to get credentials from identity service for user = {0} and account = {1}".
                    format(jdata['ad_username'], jdata['dest_account_id']))
            credentials = get_credentials_for_role(jdata['ad_username'], jdata['dest_account_id'], jdata['dest_account_role'], 
                            password, jdata['identity_service_url'], jdata['cert_verify_flag'])
        else:
            logger.info("Trying to get credentials by using IAM assume role arn with target account  [{0}]...".format(jdata['sts_tgt_role_arn']))
            credentials = get_credentials_for_role_arn(jdata['sts_tgt_role_arn'], "DMSTaskCopyToolSession")
        dms2 = boto3.client('dms', region_name=jdata['dest_account_region'], aws_access_key_id=credentials['awsAccessKey'],
                            aws_secret_access_key=credentials['awsSecretKey'],
                            aws_session_token=credentials['awsSessionToken'])
        logger.info("dms2={0}".format(dms2))
        dest_ec2 = boto3.client('ec2', region_name=jdata['dest_account_region'], aws_access_key_id=credentials['awsAccessKey'],
                            aws_secret_access_key=credentials['awsSecretKey'],
                            aws_session_token=credentials['awsSessionToken'])
        dms2resources = boto3.client('resourcegroupstaggingapi', region_name=jdata['dest_account_region'], aws_access_key_id=credentials['awsAccessKey'],
                            aws_secret_access_key=credentials['awsSecretKey'],
                            aws_session_token=credentials['awsSessionToken'])
    
    # Collect required settings from source account
    dir_name = jdata['dms_task_import_export_subdir']
    dw = datawrapper.DataWrapper( "rep-tasks", options["mode"], logger, "src_acct_dat")
    response = ""
    jepresdata = ""
    data = ""
    json_ep_filters = ""
    epcount = 0
    if options["mode"] in "export":
        # Retrieve the resources with applicable env tags for source account...
        logger.info ("Trying to retrieve DMS tasks by filtering on tags...{0}".format(jdata['rep_task_tag_filters']))
        # Fetch tag data for all tagged (or previously tagged) RDS DB instances
        paginator = dms1resources.get_paginator('get_resources')
        task_mappings = chain.from_iterable(
            page['ResourceTagMappingList']
            for page in paginator.paginate(TagFilters=jdata['rep_task_tag_filters'],
            ResourceTypeFilters=[
                'dms:task',
            ],
            ResourcesPerPage=100)
        )
        json_task_filters = ""
        taskcount = 0
        for task in task_mappings:
            json_task_filters += task['ResourceARN'] + ","
            taskcount = taskcount + 1
        if taskcount == 0:
            logger.info("No DMS Tasks found in Source Account {0} to copy! Exiting...".format(jdata['src_account_id']))
            sys.exit(1)
        json_task_filters = json_task_filters[:-1]
        logger.info("json_task_filters={0}".format(json_task_filters))
        response = dms1.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values':
                        list(json_task_filters.split(","))
                },
            ]
        )
        logger.info ("DMS1 describe_replication_tasks = {0}".format(response))
        dw.write_task_data_to_file(response)

        try:
            response = dms1resources.get_resources(
                TagFilters=jdata['rep_task_tag_filters'],
                ResourceTypeFilters=[
                    'dms:endpoint',
                ]
            )
            #print("After get resources = ", response)
            jepresdata = response
            logger.info("Source Account Endpoints Filtered By Tag (jepresdata)= {0}".format(jepresdata))
            dw.setTaskName("end-point-tags")
            dw.write_task_data_to_file(jepresdata)
        except Exception as e:
            #print("ERROR:",str(e))
            logger.error("ERROR:{0}".format(str(e)))

        epcount = sum([len(jepresdata['ResourceTagMappingList'])])
        for ep in range(epcount):
            json_ep_filters += jepresdata['ResourceTagMappingList'][ep]['ResourceARN'] + ","
        json_ep_filters = json_ep_filters[:-1]
        logger.info("Source Account Endpoints ARN Filtered (json_ep_filters) = {0}".format(json_ep_filters))

        epresponse = dms1.describe_endpoints(
            Filters=[
                {
                    'Name': 'endpoint-arn',
                    'Values':
                        list(json_ep_filters.split(","))
                },
            ]
        )
        logger.info("DMS1 describe_endpoints on selected ARN's = {0}".format(epresponse))
        dw.setTaskName("end-points")
        dw.write_task_data_to_file(epresponse)

        #Src acct replication instance information
        # Call rep instance describe to get instance arn
        try:
            # Retrieve the resources with applicable env tags for source account...
            logger.info("Trying to retrieve DMS tasks by filtering on tags...{0}".format(jdata['rep_task_tag_filters']))
            response = dms1resources.get_resources(
                TagFilters=jdata['rep_task_tag_filters'],
                ResourceTypeFilters=[
                    'dms:rep',
                ]
            )

            logger.info("Get resources Tag={0}".format(response))
            jresrepdata = response
            instcount = sum([len(jresrepdata['ResourceTagMappingList'])])
            json_inst_filters = ""
            logger.info("Count of Replication Instances to created = {0}".format(instcount))
            if instcount == 0:
                logger.info("No replication instance tagged from source account = {0}."
                            " Continuing with task createion...".format(jdata['src_account_id']))
            else:
                for inst in range(instcount):
                    json_inst_filters += jresrepdata['ResourceTagMappingList'][inst]['ResourceARN'] + ","
                json_inst_filters = json_inst_filters[:-1]
                logger.info("json_inst_filters={0}".format(json_inst_filters))
                rep_inst_response = dms1.describe_replication_instances(
                    Filters=[
                        {
                            'Name': 'replication-instance-arn',
                            'Values':
                                list(json_inst_filters.split(","))
                        },
                    ]
                )
                logger.info("DMS1 describe_replication_instances = {0}".format(rep_inst_response))
                dw.setTaskName("rep-insts")
                dw.write_task_data_to_file(rep_inst_response)
            ########################################################################################

        except ClientError as e:
            logger.info("ERROR target Endpoint = {0}".format(str(e)))
            if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                if str(e).find("No Endpoints found"):
                    logger.info("Exception:{0}".format(str(e)))
                else:
                    logger.error("ERROR:{0}".format(str(e)))
                    logger.info("Exiting...")
                    sys.exit(1)
            else:
                logger.info("ERROR:{0}".format(str(e)))
                logger.info("Exiting...")
                sys.exit(1)
        except Exception as e:
            logger.error("ERROR:{0}".format(str(e)))
            logger.info("Exiting...")
            sys.exit(1)

    else:
        dw.setTaskName("rep-tasks")
        data = dw.read_task_data_from_file()
        dw.setTaskName("end-point-tags")
        jepresdata = dw.read_task_data_from_file()
        dw.setTaskName("end-points")
        epresponse = dw.read_task_data_from_file()
        dw.setTaskName("rep-insts")
        rep_inst_response = dw.read_task_data_from_file()
    ##############################EXPORT SECTION##############################
    
    if options["mode"] in "export":
        dw.setTaskName("rep-tasks")
        data = dw.read_task_data_from_file()
        count = sum([len(data['ReplicationTasks'])])
        for x in range(count):
            RepTaskIdentifier = data['ReplicationTasks'][x]['ReplicationTaskIdentifier']
            RepTaskInstArn = data['ReplicationTasks'][x]['ReplicationInstanceArn']

            TableMapping_content = data['ReplicationTasks'][x]['TableMappings']
            ReplicationTaskSettings_content = data['ReplicationTasks'][x]['ReplicationTaskSettings']

            # write data to file
            tablmappings = tablemappings.TableMappings(RepTaskIdentifier, options["mode"], logger, dir_name)
            tablmappings.setTableMappingsToDMSResponse(TableMapping_content)
            tablmappings.writeTableMappingsToFile()

            replTaskSettings = replicationtasksettings.ReplicationTaskSettings(RepTaskIdentifier, options["mode"], logger, dir_name)
            replTaskSettings.setReplicationTaskSettingsToDMSResponse(ReplicationTaskSettings_content)
            replTaskSettings.writeReplicationTaskSettingsToFile()
        logger.info("TableMapping/ReplicationTaskSetting files are saved/exported to disk.")
        logger.info("Re-run the script with mode set to import for using the settings from the files saved to disk.")
        logger.info("Exiting...Bye!")
        sys.exit(0)

    ################################CLEAN UP SECTION########################
    # if you are re-running for a second time on the same destination or target account
    # select 'y' to perform cleanup!
    choice = input("Do you want to perform cleanup of resources before starting? (y/n) : ")
    if (choice.lower() in "y"):
        logger.info("Trying to clean up existing DMS objects in destination account: {0}".format(jdata['dest_account_id']))
        #check_and_delete_resources(data, dms2)
        cleanresources = cleancreatedresources.CleanResources(dms2, dms2resources,
                                                                    jdata['rep_task_tag_filters'][0]['Key'],
                                                                    jdata['dest_environment'], logger, max_workers)
        cleanresources.cleanRepTasks()
        #sys.exit(1)
        cleanresources.cleanRepInstances()
        cleanresources.cleanEndpoints()
        logger.info("Clean up completed.")
    choice = input("Do you wish to exit after cleanup? (y/n) : ")
    if (choice.lower() in "y"):
        sys.exit(0)
    #################################IMPORT SECTION###########################

    epcount = sum([len(jepresdata['ResourceTagMappingList'])])
    if epcount == 0:
        #logger.info("No DMS Endpoints found in Source Account {0} to copy! Exiting...".format(jdata['src_account_id']))
        logger.info("No DMS Endpoints found in Source Account {0} to copy! Continuing to run "
                    "as they could be already created in target...".format(jdata['src_account_id']))
        #sys.exit(1)
        endpoints_dict = {}
        ep_dict_count = sum([len(jdata['endpoints'])])
        for x in range(ep_dict_count):
            # logger.info(data['ReplicationTasks'][x]['ReplicationTaskIdentifier'])
            endpoints_dict[x] = {}
            # get replication ID from ARN...
            endpoints_dict[x]['endpoint_arn'] = jdata['endpoints'][x]['endpoint_arn']
            endpoints_dict[x]['endpoint_identifier'] = jdata['endpoints'][x]['endpoint_identifier']
            endpoints_dict[x]['engine_name'] = jdata['endpoints'][x]['engine_name']
            endpoints_dict[x]['user_name'] = jdata['endpoints'][x]['user_name']
            if ( jdata['endpoints'][x]['endpoint_type'] in 'target' and jdata['endpoints'][x]['password'] != "" ):
                endpoints_dict[x]['password'] = jdata['endpoints'][x]['password']
            elif ( jdata['endpoints'][x]['endpoint_type'] in 'target' ):
                endpoints_dict[x]['password'] = options['tgt_ep_passwd']
            if ( jdata['endpoints'][x]['endpoint_type'] in 'source' and jdata['endpoints'][x]['password'] != "" ):
                endpoints_dict[x]['password'] = jdata['endpoints'][x]['password']
            elif ( jdata['endpoints'][x]['endpoint_type'] in 'source' ):
                endpoints_dict[x]['password'] = options['src_ep_passwd']
            # endpoints_dict[x]['password'] = jdata['endpoints'][x]['password']
            endpoints_dict[x]['server_name'] = jdata['endpoints'][x]['server_name']
            endpoints_dict[x]['database_port'] = jdata['endpoints'][x]['database_port']
            endpoints_dict[x]['database_name'] = jdata['endpoints'][x]['database_name']
            endpoints_dict[x]['endpoint_type'] = jdata['endpoints'][x]['endpoint_type']
            endpoints_dict[x]['endpoint_new_arn'] = ""
            endpoints_dict[x]['endpoint_identifier_new'] = jdata['endpoints'][x]['endpoint_identifier_new']
            endpoints_dict[x]['extra_conn_attr'] = jdata['endpoints'][x]['extra_conn_attr']
            endpoints_dict[x]['endpoint_identifier_special_chars']  = jdata['endpoints'][x]['endpoint_identifier_special_chars']
    else:
        endpoints_dict = {}
        ep_dict_count = sum([len(epresponse['Endpoints'])])

        for x in range(ep_dict_count):
            # logger.info(data['ReplicationTasks'][x]['ReplicationTaskIdentifier'])
            endpoints_dict[x] = {}
            # get replication ID from ARN...
            endpoints_dict[x]['endpoint_arn'] = epresponse['Endpoints'][x]['EndpointArn']
            endpoints_dict[x]['endpoint_identifier'] = epresponse['Endpoints'][x]['EndpointIdentifier']
            endpoints_dict[x]['engine_name'] = epresponse['Endpoints'][x]['EngineName']
            endpoints_dict[x]['user_name'] = epresponse['Endpoints'][x]['Username']
            endpoints_dict[x]['password'] = None
            if ( jdata['endpoints'][x%2]['endpoint_type'] in 'target' and jdata['endpoints'][x%2]['password'] != "" ):
                endpoints_dict[x]['password'] = jdata['endpoints'][x%2]['password']
            elif ( jdata['endpoints'][x%2]['endpoint_type'] in 'target' ):
                endpoints_dict[x]['password'] = options['tgt_ep_passwd']
            if ( jdata['endpoints'][x%2]['endpoint_type'] in 'source' and jdata['endpoints'][x%2]['password'] != "" ):
                endpoints_dict[x]['password'] = jdata['endpoints'][x%2]['password']
            elif ( jdata['endpoints'][x%2]['endpoint_type'] in 'source' ):
                endpoints_dict[x]['password'] = options['src_ep_passwd']
            endpoints_dict[x]['server_name'] = epresponse['Endpoints'][x]['ServerName']
            endpoints_dict[x]['database_port'] = epresponse['Endpoints'][x]['Port']
            endpoints_dict[x]['database_name'] = epresponse['Endpoints'][x]['DatabaseName']
            endpoints_dict[x]['endpoint_type'] = epresponse['Endpoints'][x]['EndpointType']
            try:
                endpoints_dict[x]['endpoint_sqlserver_settings'] = epresponse['Endpoints'][x]['MicrosoftSQLServerSettings']
            except Exception as e:
                logger.info("Exception - MicrosoftSQLServerSettings = {0}".format(str(e)) )
                endpoints_dict[x]['endpoint_sqlserver_settings'] = ""
            try:
                endpoints_dict[x]['endpoint_postgres_settings'] = epresponse['Endpoints'][x]['PostgreSQLSettings']
            except Exception as e:
                logger.info("Exception - PostgreSQLSettings = {0}".format(str(e)) )
                endpoints_dict[x]['endpoint_postgres_settings'] = ""
            endpoints_dict[x]['endpoint_new_arn'] = ""
            endpoints_dict[x]['endpoint_identifier_new'] = ""
            try:
                endpoints_dict[x]['extra_conn_attr'] = epresponse['Endpoints'][x]['ExtraConnectionAttributes']
            except Exception as e:
                logger.info("Exception - ExtraConnectionAttributes = {0}".format(str(e)) )
                endpoints_dict[x]['extra_conn_attr'] = ""
    logger.info("In Memory endpoints_dict = {0}".format(endpoints_dict))
    update_endpoints_dict(endpoints_dict,jdata)
    #logger.info("jdata = {0}".format(jdata))
    logger.info("After updating in Memory endpoints_dict = {0}".format(endpoints_dict))

    ########################################################################

    ep_count = sum([len(endpoints_dict)])
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        future_to_eps = {executor.submit(createEndpoints, endpoints_dict, inst): inst for inst in range(ep_count)}
        for future in concurrent.futures.as_completed(future_to_eps):
            inst = future_to_eps[future]
            try:
                future_ep_result = future.result()
            except Exception as exc:
                logger.info("Error in createEndpoints: %s".format(str(exc)))
            else:
                logger.info("Completed thread for creating endpoints - {0}".format(inst+1))
                logger.info("Future result = {0}".format(future_ep_result))
    logger.info("Updated endpoints_dict = {0}".format(endpoints_dict))

    #########################################################################
    
    logger.info("Source Acct Replication Instance Details = {0}".format(rep_inst_response))

    #Get default security group
    sec_grp_response = dest_ec2.describe_security_groups(
        Filters=[
            {
                'Name': 'group-name',
                'Values': [
                    'default'
                ]
            },
        ]
    )

    logger.info("Sec group response for default secgroup = {0}".format(sec_grp_response))

    #########################################################################

    #Create the replication instances!
    rep_inst_dict = {}
    count = sum([len(data['ReplicationTasks'])])

    logger.info('No of DMS Tasks to Copy: {0}'.format(str(count)))
    task_id = 0
    for x in range(count):
        # logger.info(data['ReplicationTasks'][x]['ReplicationTaskIdentifier'])
        rep_inst_dict[x] = {}
        # get replication ID from ARN...
        rep_inst_dict[x]['rep_inst_arn'] = data['ReplicationTasks'][x]['ReplicationInstanceArn']
        rep_inst_dict[x]['rep_inst_new_arn'] = "empty"
        #rep_inst_dict[x]['rep_inst_id'] = "empty"
        rep_inst_dict[x]['rep_inst_id'] = data['ReplicationTasks'][x]['ReplicationInstanceArn']
        rep_inst_dict[x]['rep_instance_identifier_new'] = ""
    src_rep_inst_count = sum([len(rep_inst_response['ReplicationInstances'])])
    logger.info('Source Acct Repl Instance Details = {0}'.format(rep_inst_dict))
    logger.info('No of Repl Insts to Create: {0}'.format(str(src_rep_inst_count)))
    # for each element in describe_replication_instance results,
    #
    for i in range(src_rep_inst_count):
        RepInstId = rep_inst_response['ReplicationInstances'][i]['ReplicationInstanceIdentifier']
        RepInstArn = rep_inst_response['ReplicationInstances'][i]['ReplicationInstanceArn']
        index = get_rep_inst_elem(rep_inst_dict,count,RepInstArn)
        rep_inst_dict[index]['rep_inst_id'] = RepInstId
        rep_inst_dict[index]['rep_inst_class'] = rep_inst_response['ReplicationInstances'][i]['ReplicationInstanceClass']
        rep_inst_dict[index]['rep_engine_version'] = rep_inst_response['ReplicationInstances'][i]['EngineVersion']
        rep_inst_dict[index]['rep_inst_storage'] = rep_inst_response['ReplicationInstances'][i]['AllocatedStorage']
        rep_inst_dict[index]['rep_inst_az'] = rep_inst_response['ReplicationInstances'][i]['AvailabilityZone']
        rep_inst_dict[index]['rep_inst_maint'] = rep_inst_response['ReplicationInstances'][i]['PreferredMaintenanceWindow']
        rep_inst_dict[index]['rep_inst_maz'] = rep_inst_response['ReplicationInstances'][i]['MultiAZ']
        #rep_inst_dict[index]['rep_inst_sg'] = rep_inst_response['ReplicationInstances'][i]['VpcSecurityGroups'][0]['VpcSecurityGroupId']
        rep_inst_dict[index]['rep_inst_sg'] = sec_grp_response['SecurityGroups'][0]['GroupId']
        #rep_inst_dict[index]['rep_inst_vpc'] = rep_inst_response['ReplicationInstances'][i]['ReplicationSubnetGroup']['ReplicationSubnetGroupIdentifier']
        rep_inst_dict[index]['rep_inst_vpc'] = "default-"+sec_grp_response['SecurityGroups'][0]['VpcId']
        rep_inst_dict[index]['rep_inst_pub'] = rep_inst_response['ReplicationInstances'][i]['PubliclyAccessible']
        rep_inst_dict[index]['rep_inst_new_arn'] = ""
        logger.info("rep_inst_dict[{0}] = {1}".format(index,rep_inst_dict[index]))

    # Now iterate over the config rep tasks and match them with whatever we have in memory
    rep_inst_jdata_count = sum([len(jdata['rep_instances'])])
    for k in range(rep_inst_jdata_count):
        # logger.info(data['ReplicationTasks'][x]['ReplicationTaskIdentifier'])
        if jdata['rep_instances'][k]['rep_instance_arn'] != "":
            if jdata['rep_instances'][k]['rep_instance_identifier'] != "":
                index = get_rep_inst_elem_from_id(rep_inst_dict, sum([len(rep_inst_dict)]), jdata['rep_instances'][k]['rep_instance_identifier'])
                if index != -1:
                    rep_inst_dict[index]['rep_inst_new_arn'] = jdata['rep_instances'][k]['rep_instance_arn']
                else:
                    #Error....no element with matching identifier found from source acct!
                    logger.fatal("ERROR - No Replication Instance Identifier = {0}, found "
                                 "from Source Account".format(jdata['rep_instances'][k]['rep_instance_identifier']))
            else:
                #error...identifier is mandatory when ARN of target acct instance is provided!
                logger.fatal("ERROR - No Replication Instance Identifier defined "
                             "for rep_instance_arn = {0}".format(jdata['rep_instances'][k]['rep_instance_arn']))
    logger.info("Before update rep_inst_dict contents={0}".format(rep_inst_dict))
    update_rep_inst_dict(rep_inst_dict, jdata)
    logger.info("After update replication inst dict call = {0}".format(rep_inst_dict))
    #########################################################################
    rep_inst_count = sum([len(rep_inst_dict)])
    logger.info("Trying to create a replication instance for each DMS Task count = {0}".format(rep_inst_count))
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        future_to_rep_insts = {executor.submit(createRepInstances, rep_inst_dict, inst): inst for inst in range(rep_inst_count)}
        for future in concurrent.futures.as_completed(future_to_rep_insts):
            inst = future_to_rep_insts[future]
            try:
                future_result = future.result()
            except Exception as exc:
                logger.info("Error: %s".format(str(exc)))
            else:
                logger.info("Completed thread for creating replication instances - {0}".format(inst+1))
                logger.info("Future result = {0}".format(future_result))
    logger.info("Updated rep_inst_dict = {0}".format(rep_inst_dict))
    #########################################################################
    ep_count = sum([len(endpoints_dict)])
    count = sum([len(data['ReplicationTasks'])])
    logger.info('No of DMS Tasks to Copy: {0}'.format(str(count) ))
    task_id = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        future_to_rep_tasks = {executor.submit(createRepTasks, data, x, options): x for x in range(count)}
        for future in concurrent.futures.as_completed(future_to_rep_tasks):
            x = future_to_rep_tasks[future]
            try:
                future_task_result = future.result()
            except Exception as exc:
                logger.info("Error: %s".format(str(exc)))
            else:
                logger.info("Starting thread for Creating Replication Tasks - {0}".format(x + 1))
                logger.info("Future result = {0}".format(future_task_result))

    logger.info("Copied count = {0} , Replication tasks to Destination account = {1}".format(count,jdata['dest_account_id']))
    logger.info("Script completed and exiting...Bye!")
