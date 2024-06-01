import redis, datetime

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# all_keys = redis_client.keys()
# print('--- all keys ---')
# for key in all_keys: print(key)

# val_list = []
# for key in all_keys:
#     if redis_client.type(key) == 'hash':
#         retrieved_data = redis_client.hgetall(key)
#         val_list.append(retrieved_data)
#     else:
#         value = redis_client.get(key)
#         val_list.append(value)

# print(len(all_keys))
# print(len(val_list))
# print(type(val_list[0]))

# for k, v in val_list[0].items():
#     if k == "Msg":
#         print(f"{k}:")
#         continue
#     print(f"{k}: {v}")
# # print(val_list[0]['Msg'])

def get_filenames():
    return redis_client.lrange("filenames", 0, -1)

def query_by_filename(filename):
    log_names = redis_client.zrange(filename, 0, -1)
    print(len(log_names))
    # for log_name in log_names: print(log_name)
    # return [redis_client.hgetall(log_name) for log_name in log_names]

def query_by_timestamp(filename, start_time, end_time):
    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()
    log_names = redis_client.zrangebyscore(filename, start_timestamp, end_timestamp)
    for log_name in log_names: print(log_name)
    # return [redis_client.hgetall(log_name) for log_name in log_names]

def query_by_type_id(filename, type_id):
    log_names = redis_client.zrange(filename, 0, -1)
    result = []
    for log_name in log_names:
        if log_name.endswith(f":{type_id}"):
            result.append(redis_client.hgetall(log_name))
            print(log_name)
    # return result

print('--- get filenames ---')
filenames = get_filenames()
print(filenames)

print('--- query by filename ---')
for filename in filenames:
    print(filename)
    query_by_filename(filename)
filename = '20201116_153133_Xiaomi-Mi10_46000.mi2log'
query_by_filename(filename)

# filename = '20201115_181637_Xiaomi-Mi10_46000.mi2log'
start_time = datetime.datetime.strptime('2020-11-16-07-22-50-795809', '%Y-%m-%d-%H-%M-%S-%f')
end_time = datetime.datetime.strptime('2020-11-16-07-23-00-337162', '%Y-%m-%d-%H-%M-%S-%f')
print('--- query by timestamp ---')
query_by_timestamp(filename, start_time, end_time)
print('--- query by type_id ---')
# type_id = '5G_NR_RRC_OTA_Packet'
type_id = 'LTE_RRC_Serv_Cell_Info'
query_by_type_id(filename, type_id)


