#!/usr/bin/env python
#
#   tablemappings.py
#
#   Description - A class to write/read the tablemappings file for DMS tasks.
#
#   Created by: AWS Proserve Team
#   Created on: 08/03/2021
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
#import argparse
#import configparser
import time
import datetime
import inspect
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TableMappings:
    def  __init__(self, task, mode, logger, dir_name):
        self.filename = "tm-"+task + ".json"
        self.data = json.dumps("")
        self.mode = mode
        self.filedir = os.getcwd() + os.sep + dir_name + os.sep
        if not os.path.exists(self.filedir):
            os.mkdir(self.filedir)
        self.logger = logger
        self.dir_name = dir_name

    def getTableMappingsFromDMSResponse(self, dmsResponse):
        return self.data

    def setTableMappingsToDMSResponse(self, data):
        self.data = data

    def readTableMappingsFromFile(self):
        try:
            with open(self.filedir + self.filename) as f:
                data = json.load(f)
                f.close()
        except Exception as e:
            print("ERROR: Opening file failed. ",str(e))
            sys.exit(1)
        return data

    def writeTableMappingsToFile(self):
        try:
            with open(self.filedir + self.filename, 'w') as f:
                json.dump(self.data, f)
                f.close()
        except Exception as e:
            print("ERROR: Opening file failed. ",str(e))
            sys.exit(1)
