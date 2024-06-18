import os, redis, csv, json, datetime
import numpy as np
import shutil

from mobile_insight.analyzer.analyzer import *
from mobile_insight.monitor import OfflineReplayer

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

class myAnalyzer(Analyzer):
    def __init__(self):
        Analyzer.__init__(self)
        self.add_source_callback(self.__msg_callback)
        self.log_item_list = []

    def set_source(self, source):
        Analyzer.set_source(self, source)
        source.enable_log_all()

    def __msg_callback(self, msg):
        data = msg.data.decode()
        self.log_item_list.append(data)

def my_analysis(input_path):
    src = OfflineReplayer()
    src.set_input_path(input_path)
    analyzer = myAnalyzer()
    analyzer.set_source(src)
    src.run()
    return analyzer

def check_eligibility(filename):
    files = redis_client.lrange("filenames", 0, -1)
    if filename in files:
        return False
    else:
        return True

def add_filename_to_list(filename):
    redis_client.rpush("filenames", filename)

def add_log_item(filename, log_item, item_number):
    timestamp = log_item['timestamp'].strftime('%Y-%m-%d-%H-%M-%S-%f')
    type_id = log_item['type_id']
    item_number = str(item_number)
    log_name = f"{filename}:{item_number}:{timestamp}:{type_id}"
    redis_client.zadd(filename, {log_name: log_item['timestamp'].timestamp()})    # store in sorted set
    log_item['timestamp'] = timestamp
    json_str = json.dumps(log_item, indent=4)    # serialize the log item
    redis_client.set(log_name, json_str)    # store in string
    print(log_name)


file_path = "../examples/logs/offline_log_examples/20201116_162549_Xiaomi-Mi10_46000.mi2log"


dest_dir = "./uploads/"
shutil.copy(file_path, dest_dir)

filename = file_path.split("/")[-1]
print(filename)
if check_eligibility(filename) is True:
    add_filename_to_list(filename)
    stats = my_analysis(file_path)
    item_number = 1
    for item in stats.log_item_list:
        add_log_item(filename, item, item_number)
        item_number += 1
else:
    print("This file already exists.")

