import sys
import json
import os

class DataWrapper:
    def  __init__(self, task, mode, logger, dir_name):
        self.filename = "dms-"+task + ".json"
        self.data = json.dumps("")
        self.mode = mode
        self.filedir = os.getcwd() + os.sep + dir_name + os.sep
        if not os.path.exists(self.filedir):
            os.mkdir(self.filedir)
        self.logger = logger
        self.dir_name = dir_name

    def write_task_data_to_file(self, task_data):
        print("Writing data to file...")
        try:
            with open(self.filedir + self.filename, 'w') as f:
                json.dump(task_data, f, indent = 4, default=str)
                f.close()
        except Exception as e:
            print("ERROR: Opening file failed. ",str(e))
            sys.exit(1)

    def read_task_data_from_file(self):
        print("Reading data from file...")
        try:
            with open(self.filedir + self.filename) as f:
                data = json.load(f)
                f.close()
        except Exception as e:
            print("ERROR: Opening file failed. ",str(e))
            sys.exit(1)
        print("Read data = ", data)
        return data

    def setTaskName(self, taskName):
        self.task = taskName
        self.filename = "dms-"+self.task + ".json"