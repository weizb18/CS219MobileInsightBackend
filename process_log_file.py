#!/usr/bin/python

import os, redis, csv, json
import numpy as np

from mobile_insight.analyzer.analyzer import *
from mobile_insight.monitor import OfflineReplayer

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

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

def add_filename_to_list(filename):
    r.rpush("filenames", filename)

def add_log_item(filename, log_item, item_number):
    timestamp = log_item['timestamp'].strftime('%Y-%m-%d-%H-%M-%S-%f')
    type_id = log_item['type_id']
    item_number = str(item_number)
    log_name = f"{filename}:{item_number}:{timestamp}:{type_id}"
    '''
    if log_name != 'data_sample.mi2log:2017-02-28-10-40-25-905067:LTE_PDCP_UL_Cipher_Data_PDU':
        for k in log_item.keys():
            print(k)
            if k == 'Msg': break
            print(log_item[k])
    print('---')
    '''
    r.zadd(filename, {log_name: log_item['timestamp'].timestamp()})    # store in sorted set
    log_item['timestamp'] = timestamp
    json_str = json.dumps(log_item, indent=4)    # serialize the log item
    r.set(log_name, json_str)    # store in string
    # r.hset(log_name, mapping=log_item)    # store in hash

filename = '20201115_181637_Xiaomi-Mi10_46000.mi2log'
input_path = '../examples/logs/offline_log_examples/20201115_181637_Xiaomi-Mi10_46000.mi2log'

# filename = 'data_sample.mi2log'
# input_path = '../examples/logs/data_sample.mi2log'
add_filename_to_list(filename)
stats = my_analysis(input_path)
item_number = 1
for item in stats.log_item_list:
    add_log_item(filename, item, item_number)
    item_number += 1
'''
cnt = 0
filenames = os.listdir('../examples/logs/offline_log_examples/')
for filename in filenames:
    print(filename)
    add_filename_to_list(filename)
    input_path = '../examples/logs/offline_log_examples/' + filename
    stats = my_analysis(input_path)
    item_number = 1
    for item in stats.log_item_list:
        add_log_item(filename, item, item_number)
        item_number += 1
    cnt += 1
    if cnt == 5: break
'''

