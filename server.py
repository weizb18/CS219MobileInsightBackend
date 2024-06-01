import os, redis, csv, json, datetime
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
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

def get_filenames():
    return redis_client.lrange("filenames", 0, -1)

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

def query_by_filename(filename):
    log_names = redis_client.zrange(filename, 0, -1)
    return log_names
    # return [redis_client.hgetall(log_name) for log_name in log_names]

def query_file_info(filename):
    log_names = redis_client.zrange(filename, 0, -1)
    start_time_str, end_time_str = log_names[0].split(':')[2], log_names[-1].split(':')[2]
    type_ids = set()
    for log_name in log_names:
        type_id = log_name.split(':')[3]
        type_ids.add(type_id)
    type_ids = list(type_ids)
    return {"start": start_time_str, "end": end_time_str, "type_ids": type_ids}

def query_by_timestamp(filename, start_time, end_time):
    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()
    log_names = redis_client.zrangebyscore(filename, start_timestamp, end_timestamp)
    return log_names

# def query_by_type_id(filename, type_id):
#     log_names = redis_client.zrange(filename, 0, -1)
#     return [log_name for log_name in log_names if log_name.endswith(f":{type_id}")]

def query_by_type_ids(filename, type_ids):
    log_names = redis_client.zrange(filename, 0, -1)
    filtered_log_names = []
    for log_name in log_names:
        if any(log_name.endswith(type_id) for type_id in type_ids):
            filtered_log_names.append(log_name)
    return filtered_log_names

def query_item_detail(log_name):
    json_str = redis_client.get(log_name)
    return json.loads(json_str)


app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = './uploads'

@app.route('/', methods=['GET'])
def home():
    filenames = get_filenames()
    return jsonify(filenames)

@app.route('/upload_file', methods=['POST'])
def upload_file_by_user():
    if 'logfile' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['logfile']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if file:
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        add_filename_to_list(filename)
        stats = my_analysis(file_path)
        item_number = 1
        for item in stats.log_item_list:
            add_log_item(filename, item, item_number)
            item_number += 1
        return jsonify({'message': 'File successfully uploaded and processed'}), 200

@app.route('/get_items', methods=['GET'])
def get_items_by_filename():
    filename = request.args.get('filename')
    logs = query_by_filename(filename)
    return jsonify(logs)

@app.route('/get_file_info', methods=['GET'])
def get_file_info_by_filename():
    filename = request.args.get('filename')
    file_info = query_file_info(filename)
    return jsonify(file_info)

@app.route('/search_timeframe', methods=['GET'])
def get_logs_by_timestamp():
    filename = request.args.get('filename')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d-%H-%M-%S-%f')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d-%H-%M-%S-%f')
    logs = query_by_timestamp(filename, start_time, end_time)
    return jsonify(logs)

# @app.route('/search_type_id', methods=['GET'])
# def get_logs_by_type_id():
#     filename = request.args.get('filename')
#     type_id = request.args.get('type_id')
#     logs = query_by_type_id(filename, type_id)
#     return jsonify(logs)

@app.route('/search_type_ids', methods=['POST'])
def get_logs_by_type_ids():
    data = request.json
    filename, type_ids = data.get('filename', ''), data.get('type_ids', [])
    if not isinstance(type_ids, list):
        return jsonify({"error": "Invalid type id format"}), 400
    logs = query_by_type_ids(filename, type_ids)
    return jsonify(logs)

@app.route('/get_item_detail', methods=['GET'])
def get_item_detail_by_log_name():
    log_name = request.args.get('log_name')
    log = query_item_detail(log_name)
    return jsonify(log)

if __name__ == '__main__':
    app.run(port=8080, host="0.0.0.0", debug=True)

