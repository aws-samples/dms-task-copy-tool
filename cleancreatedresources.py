#!/usr/bin/env python
#
#   cleancreatedresources.py
#
#   Description - A class to clean up created DMS tasks.
#
#   Created by: AWS Proserve Team
#   Created on: 08/03/2021
#
import os
import sys
import boto3
from botocore.exceptions import ClientError
import json
import requests
import logging
import time
import datetime
import inspect
import urllib3
import threading
import concurrent.futures
from itertools import chain
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CleanResources:
    def  __init__(self, dmsconn, dmsresconn, tag_key, tag_value, logger, maxworkers):
        self.tag_key = tag_key
        self.tag_value = tag_value
        self.logger = logger
        self.dmsresconn = dmsresconn
        self.dmsconn = dmsconn
        self.maxworkers = maxworkers

    def deleteEndpoints(self, jepdata, index):
        retryFlag = True
        retryCount = 5
        while retryFlag and retryCount > 0:
            try:
                self.logger.info("Performing Endpoint Delete on ARN={0}".format(
                    jepdata['ResourceTagMappingList'][index]['ResourceARN']))
                response = self.dmsconn.delete_endpoint(
                    EndpointArn=jepdata['ResourceTagMappingList'][index]['ResourceARN'])
                self.logger.info("Sleeping for 30 secs to allow the endpoint deletion to complete...Please wait...")
                time.sleep(30)
            except ClientError as e:
                self.logger.info("ERROR Endpoint Deletion = {0}".format(str(e)))
                if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                    if str(e).find("not found"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        retryFlag = False
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
                elif e.response['Error']['Code'] == "InvalidResourceStateFault":
                    if str(e).find("already being deleted"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        self.logger.info(
                            "Sleeping for 30 seconds to allow endpoints to be deleted...Please wait...")
                        time.sleep(30)
                        retryFlag = True
                        self.logger.info(
                            "Trying to delete the endpoint. Retry Count = {0}".format(retryCount))
                        retryCount = retryCount - 1
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
            except Exception as e:
                self.logger.info("ERROR:{0}".format(str(e)))
                self.logger.info("Exiting...")
                sys.exit(1)

                ######

    def cleanEndpoints(self):
        try:
            response = self.dmsresconn.get_resources(
                TagFilters=[{'Key':self.tag_key, 'Values':[self.tag_value]}],
                ResourceTypeFilters=[
                    'dms:endpoint'
                ]
            )
            #print("After get resources = ", response)
            jepresdata = response
            self.logger.info("Source Account Endpoints Filtered By Tag (jepresdata)= {0}".format(jepresdata))
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxworkers) as executor:
                future_to_eps = {executor.submit(self.deleteEndpoints, jepresdata, i): i for i in range(sum([len(jepresdata['ResourceTagMappingList'])]))}
                for future in concurrent.futures.as_completed(future_to_eps):
                    x = future_to_eps[future]
                    try:
                        future_ep_result = future.result()
                    except Exception as exc:
                        self.logger.info("Error: %s".format(str(exc)))
                    else:
                        self.logger.info("Starting thread for cleaning endpoint - {0}".format(x + 1))
                        self.logger.info("Future result = {0}".format(future_ep_result))
        except Exception as e:
            # print("ERROR:",str(e))
            self.logger.error("ERROR:{0}".format(str(e)))

    def deleteRepInstances(self, jepresdata, i):
        retryFlag = True
        retryCount = 20
        while retryFlag and retryCount > 0:
            try:
                self.logger.info("Performing RepInstance Delete on ARN={0}".format(
                    jepresdata['ResourceTagMappingList'][i]['ResourceARN']))
                response = self.dmsconn.delete_replication_instance(
                    ReplicationInstanceArn=jepresdata['ResourceTagMappingList'][i]['ResourceARN'])
                #self.logger.info("Sleeping for 30 secs to allow the RepInstance deletion to complete...Please wait...")
                time.sleep(2)
                #retryCount = retryCount - 1
            except ClientError as e:
                self.logger.info("ERROR Replication Instance Deletion = {0}".format(str(e)))
                if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                    if str(e).find("not found"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        retryFlag = False
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
                elif e.response['Error']['Code'] == "InvalidResourceStateFault":
                    if str(e).find("already being deleted"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        self.logger.info(
                            "Sleeping for 30 seconds to allow Replication Instance to be deleted...Please wait...")
                        time.sleep(30)
                        retryFlag = True
                        self.logger.info(
                            "Trying to delete the Replication Instance. Retry Count = {0}".format(retryCount))
                        retryCount = retryCount - 1
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
            except Exception as e:
                self.logger.info("ERROR:{0}".format(str(e)))
                self.logger.info("Exiting...")
                sys.exit(1)

    def cleanRepInstances(self):
        try:
            response = self.dmsresconn.get_resources(
                TagFilters=[{'Key':self.tag_key, 'Values':[self.tag_value]}],
                ResourceTypeFilters=[
                    'dms:rep'
                ]
            )
            #print("After get resources = ", response)
            jepresdata = response
            self.logger.info("Source Account Replication Instances Filtered By Tag (jepresdata)= {0}".format(jepresdata))
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxworkers) as executor:
                future_to_insts = {executor.submit(self.deleteRepInstances, jepresdata, i): i for i in range(sum([len(jepresdata['ResourceTagMappingList'])]))}
                for future in concurrent.futures.as_completed(future_to_insts):
                    x = future_to_insts[future]
                    try:
                        future_inst_result = future.result()
                    except Exception as exc:
                        self.logger.info("Error: %s".format(str(exc)))
                    else:
                        self.logger.info("Starting thread for cleaning replication instance - {0}".format(x + 1))
                        self.logger.info("Future result = {0}".format(future_inst_result))
        except Exception as e:
            # print("ERROR:",str(e))
            self.logger.error("ERROR:{0}".format(str(e)))

    def deleteRepTasks(self, task, i):
        retryFlag = True
        retryCount = 20
        while retryFlag and retryCount > 0:
            try:
                self.logger.info("Performing RepTask Delete on ARN={0}".format(
                    task['ResourceARN']))
                response = self.dmsconn.delete_replication_task(
                    ReplicationTaskArn=task['ResourceARN'])
                #self.logger.info("Sleeping for 30 secs to allow the RepTask deletion to complete...Please wait...")
                time.sleep(2)
                retryFlag = False
            except ClientError as e:
                self.logger.info("ERROR Replication Task Deletion = {0}".format(str(e)))
                if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                    if str(e).find("not found"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        retryFlag = False
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
                elif e.response['Error']['Code'] == "InvalidResourceStateFault":
                    if str(e).find("already being deleted"):
                        self.logger.info("Exception:{0}".format(str(e)))
                        self.logger.info(
                            "Sleeping for 30 seconds to allow Replication Task to be deleted...Please wait...")
                        time.sleep(30)
                        retryFlag = True
                        self.logger.info(
                            "Trying to delete the Replication Task. Retry Count = {0}".format(retryCount))
                        retryCount = retryCount - 1
                    else:
                        self.logger.info("ERROR:{0}".format(str(e)))
                        self.logger.info("Exiting...")
                        sys.exit(1)
            except Exception as e:
                self.logger.info("ERROR:{0}".format(str(e)))
                self.logger.info("Exiting...")
                sys.exit(1)

    def cleanRepTasks(self):
        try:
            # Fetch tag data for all tagged (or previously tagged) RDS DB instances
            paginator = self.dmsresconn.get_paginator('get_resources')
            task_mappings = chain.from_iterable( page['ResourceTagMappingList']
                for page in paginator.paginate(TagFilters=[{'Key':self.tag_key, 'Values':[self.tag_value]}],
                ResourceTypeFilters=[
                    'dms:task'
                ],
               ResourcesPerPage=100) )
            i = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxworkers) as executor:
                for task in task_mappings:
                    i = i + 1
                    future_to_tasks = {executor.submit(self.deleteRepTasks, task, i)}
                    for future in concurrent.futures.as_completed(future_to_tasks):
                        try:
                            future_task_result = future.result()
                        except Exception as exc:
                            self.logger.info("Error: in cleanRepTasks [%s]".format(str(exc)))
                        else:
                            self.logger.info("Starting thread for cleaning replication task - {0}".format(i))
                            self.logger.info("Future result = {0}".format(future_task_result))

        except Exception as e:
            self.logger.error("ERROR:{0}".format(str(e)))
