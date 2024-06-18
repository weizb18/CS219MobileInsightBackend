import os, redis, csv, json, datetime
import numpy as np
from flask import Flask, request, jsonify, send_file
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

def get_filenames(page, limit):
    start = (page - 1) * limit
    end = start + limit
    files = redis_client.lrange("filenames", 0, -1)
    files.reverse()
    total_files = len(files)
    paginated_files = files[start:end]
    return paginated_files, total_files
    # return redis_client.lrange("filenames", 0, -1)

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

def query_by_filename(filename, page, limit):
    start = (page - 1) * limit
    end = start + limit - 1
    total_logs = redis_client.zcard(filename)
    logs = redis_client.zrange(filename, start, end)
    return logs, total_logs
    # log_names = redis_client.zrange(filename, 0, -1)
    # return log_names

def query_file_info(filename):
    log_names = redis_client.zrange(filename, 0, -1)
    start_time_str, end_time_str = log_names[0].split(':')[2], log_names[-1].split(':')[2]
    type_ids = set()
    for log_name in log_names:
        type_id = log_name.split(':')[3]
        type_ids.add(type_id)
    type_ids = list(type_ids)
    return {"start": start_time_str, "end": end_time_str, "type_ids": type_ids}

def query_by_timestamp_type_ids(filename, start_time, end_time, type_ids, page, limit):
    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()
    log_names = redis_client.zrangebyscore(filename, start_timestamp, end_timestamp)
    filtered_log_names = []
    for log_name in log_names:
        if any(log_name.endswith(type_id) for type_id in type_ids):
            filtered_log_names.append(log_name)
    return filtered_log_names[(page-1)*limit : page*limit], len(filtered_log_names)

def query_item_detail(log_name):
    json_str = redis_client.get(log_name)
    return json.loads(json_str)


app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['DOWNLOAD_FOLDER'] = './downloads'

@app.route('/', methods=['GET'])
def home():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 20, type=int)
    paginated_files, total_files = get_filenames(page, limit)
    return jsonify({
        'data': paginated_files,
        'total': total_files,
        'page': page,
        'pages': (total_files + limit - 1) // limit
    })
    # filenames = get_filenames()
    # return jsonify(filenames)

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

@app.route('/get_file_info', methods=['GET'])
def get_file_info_by_filename():
    filename = request.args.get('filename')
    file_info = query_file_info(filename)
    return jsonify(file_info)

@app.route('/search', methods=['POST'])
def get_logs_by_timestamp_type_ids():
    data = request.json
    page = int(data.get('page', 1))
    limit = int(data.get('limit', 20))
    filename = data.get('filename', '')
    start_time, end_time = data.get('start_time', ''), data.get('end_time', '')
    type_ids = data.get('type_ids', [])
    if not isinstance(type_ids, list):
        return jsonify({"error": "Invalid type id format"}), 400
    
    file_info = query_file_info(filename)
    if start_time == '': start_time = file_info['start']
    if end_time == '': end_time = file_info['end']
    if len(type_ids) == 0: type_ids = file_info['type_ids']
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d-%H-%M-%S-%f')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d-%H-%M-%S-%f')
    
    logs, total_logs = query_by_timestamp_type_ids(filename, start_time, end_time, type_ids, page, limit)
    return jsonify({
        'data': logs,
        'total': total_logs,
        'page': page,
        'pages': (total_logs + limit - 1) // limit
    })

@app.route('/get_item_detail', methods=['GET'])
def get_item_detail_by_log_name():
    log_name = request.args.get('log_name')
    log = query_item_detail(log_name)
    return jsonify(log)

@app.route('/download_filtered_logfile', methods=['POST'])
def get_filtered_logfile():
    data = request.json
    filename, type_ids = data.get('filename', ''), data.get('type_ids', [])
    if not isinstance(type_ids, list):
        return jsonify({"error": "Invalid type id format"}), 400
    original_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    processed_file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], f'processed_{filename}')
    src = OfflineReplayer()
    src.set_input_path(original_file_path)
    for type_id in type_ids:
        src.enable_log(type_id)
    src.save_log_as(processed_file_path)
    src.run()
    return send_file(processed_file_path, as_attachment=True)
    

if __name__ == '__main__':
    app.run(port=8080, host="0.0.0.0", debug=True)

