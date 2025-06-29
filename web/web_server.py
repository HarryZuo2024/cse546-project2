# File: web_server.py version 2.0 release 2025-06-28 by Wenguang Zuo
# wget https://raw.githubusercontent.com/HarryZuo2024/cse546-project2/refs/heads/main/web/web_server.py
# wget https://raw.githubusercontent.com/HarryZuo2024/cse546-project2/refs/heads/main/web/web_server_config.json

from collections import defaultdict, UserDict
from threading import RLock
import threading
import time
import logging
import json
from flask import Flask, request, jsonify, Response
import boto3
from botocore.config import Config
import uuid
from functools import wraps
import os


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 通过环境变量指定配置文件路径
# 如果是Windows 系统
if os.name == 'nt':
    CONFIG_PATH = os.environ.get('WEB_SERVER_CONFIG_PATH', r".\code\web\web_server_config.json")
# 如果是Linux 系统
else:
    CONFIG_PATH = os.environ.get('WEB_SERVER_CONFIG_PATH', '/home/us2-user/web/web_server_config.json')

# 读取配置文件
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    logger.error("未找到配置文件 web_server_config.json")
    raise

# 线程安全字典（增强版）
# class ThreadSafeDict:
#     def __init__(self):
#         self.dict = defaultdict(dict)
#         self.lock = RLock()  # 使用可重入锁
#     def __getitem__(self, key):
#         with self.lock:
#             return self.dict[key]
#     def __setitem__(self, key, value):
#         with self.lock:
#             self.dict[key] = value
#     def __delitem__(self, key):
#         with self.lock:
#             del self.dict[key]
#     def get(self, key, default=None):
#         with self.lock:
#             return self.dict.get(key, default)
#     def items(self):
#         with self.lock:
#             return list(self.dict.items())
#     def pop(self, key, default=None):
#         with self.lock:
#             return self.dict.pop(key, default)
#     def update(self, other):
#         with self.lock:
#             self.dict.update(other)
#     def __contains__(self, key):
#         with self.lock:
#             return key in self.dict
        
class SafeUserDict(UserDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = RLock()
    def __getitem__(self, key):
        with self.lock:
            return super().__getitem__(key)
    def __setitem__(self, key, value):
        with self.lock:
            super().__setitem__(key, value)
    def __delitem__(self, key):
        with self.lock:
            super().__delitem__(key)
    def get(self, key, default=None):
        with self.lock:
            return super().get(key, default)
    def items(self):
        with self.lock:
            return list(super().items())
    def pop(self, key, default=None):
        with self.lock:
            return super().pop(key, default)
    def update(self, other):
        with self.lock:
            super().update(other)
    def __contains__(self, key):
        with self.lock:
            return super().__contains__(key)
        
# AWS 配置（带默认值）
AWS_REGION = config.get("AWS_REGION", "us-east-1")
INPUT_BUCKET = config.get("INPUT_BUCKET", "project2-input-bucket-abc")
OUTPUT_BUCKET = config.get("OUTPUT_BUCKET", "project2-output-bucket-xyz")
REQUEST_QUEUE_URL = config.get("REQUEST_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/257288819129/request-queue")
RESPONSE_QUEUE_URL = config.get("RESPONSE_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/257288819129/response-queue")

# 性能优化参数（带默认值）
POLLING_INTERVAL = config.get("POLLING_INTERVAL", 0.5)
SQS_MAX_MESSAGES = config.get("SQS_MAX_MESSAGES", 10)
CLEANUP_INTERVAL = config.get("CLEANUP_INTERVAL", 300)
REQUEST_TIMEOUT = config.get("REQUEST_TIMEOUT", 360)
LONG_POLL_TIMEOUT = config.get("LONG_POLL_TIMEOUT", 300)
INITIAL_POLLING_DELAY = config.get("INITIAL_POLLING_DELAY", 10)

app = Flask(__name__)

# 配置重试策略
s3_config = Config(
    retries={
        'max_attempts': 5,
        'mode': 'adaptive'
    },
    max_pool_connections=100
)

# 记录所有请求及其状态
# request_records = ThreadSafeDict()
request_records = SafeUserDict()

# 创建锁
s3_lock = RLock()
sqs_request_lock = RLock()  # 保护请求队列操作
sqs_response_lock = RLock()  # 保护响应队列操作
shutdown_event = threading.Event()

# 统一错误处理装饰器
def handle_errors(func):
    @wraps(func)  # 新增：保留原始函数元数据
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"API处理错误: {str(e)}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    return wrapper

# 处理请求状态检查和响应构建
def process_request_status(request_id, poll_timeout=LONG_POLL_TIMEOUT):
    """处理请求状态并返回标准化响应"""
    # 获取请求记录
    record = request_records.get(request_id)
    if not record:
        return jsonify({
            'request_id': request_id,
            'status': 'not_found',
            'message': 'Request ID not found'
        }), 404
    
    # 长轮询逻辑
    start_time = time.time()
    while (time.time() - start_time < poll_timeout and 
           record['status'] == 'pending' and 
           not shutdown_event.is_set()):
        time.sleep(POLLING_INTERVAL)
        record = request_records.get(request_id)  # 重新获取最新状态
    
    # 检查请求是否超时
    if record['status'] == 'pending' and (time.time() - record['timestamp'] > REQUEST_TIMEOUT):
        request_records[request_id]['status'] = 'timeout'
        record = request_records[request_id]
    
    # 构建并返回标准化响应
    if record['status'] == 'completed':
        return Response(record['result'], content_type='text/plain'), 200
    else:
        return jsonify({
            'request_id': request_id,
            'status': record['status'],
            'message': 'Result not ready yet'
        }), 202

@app.route('/classify', methods=['POST'])
@handle_errors
def upload_file():
    logger.info("收到新的分类请求")
    
    if 'myfile' not in request.files:
        logger.warning("请求缺少文件部分")
        return 'No myfile part', 400
    
    file = request.files['myfile']
    if not file.filename:
        logger.warning("请求包含空文件名")
        return 'No selected file', 400
    
    # 生成唯一ID
    filename = f"{uuid.uuid4()}_{file.filename}"
    request_id = str(uuid.uuid4())
    logger.info(f"生成文件名: {filename}, RequestID: {request_id}")
    
    # 上传到S3
    with s3_lock:
        s3 = boto3.client('s3', region_name=AWS_REGION, config=s3_config)
        s3.upload_fileobj(file, INPUT_BUCKET, filename)
    logger.info(f"文件上传成功: {INPUT_BUCKET}/{filename}")
    
    # 发送到SQS请求队列
    with sqs_request_lock:
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        sqs.send_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MessageBody=json.dumps({
                'filename': filename,
                'request_id': request_id,
                'timestamp': time.time()
            }),
            MessageAttributes={
                'request_id': {
                    'StringValue': request_id,
                    'DataType': 'String'
                }
            }
        )
    logger.info(f"请求发送到SQS队列: {REQUEST_QUEUE_URL}")
    
    # 记录请求状态
    request_records[request_id] = {
        'filename': filename,
        'status': 'pending',
        'timestamp': time.time(),
        'result': None
    }

    # 添加初始延迟（让worker有时间处理）
    logger.debug(f"等待 {INITIAL_POLLING_DELAY} 秒后开始轮询结果")
    time.sleep(INITIAL_POLLING_DELAY)
        
    # 获取长轮询超时参数
    poll_timeout = int(request.args.get('timeout', LONG_POLL_TIMEOUT))
    # 使用公共函数处理状态响应
    return process_request_status(request_id, poll_timeout)    

@app.route('/status/<request_id>', methods=['GET'])
@handle_errors
def get_status(request_id):
    """长轮询检查请求状态"""
    # 获取长轮询超时参数
    poll_timeout = int(request.args.get('timeout', LONG_POLL_TIMEOUT))
    # 使用公共函数处理状态响应
    return process_request_status(request_id, poll_timeout)

@app.route('/result/<filename>', methods=['GET'])
@handle_errors
def get_result(filename):
    """通过文件名获取结果"""
    logger.info(f"获取结果请求: {filename}")
    
    try:
        with s3_lock:
            s3 = boto3.client('s3', region_name=AWS_REGION, config=s3_config)
            response = s3.get_object(Bucket=OUTPUT_BUCKET, Key=filename)
        result = response['Body'].read().decode('utf-8')
        
        logger.info(f"成功返回结果: {filename}")
        return jsonify({
            'filename': filename,
            'result': result
        })
        
    except s3.exceptions.NoSuchKey:
        logger.warning(f"结果尚未就绪: {filename}")
        return jsonify({
            'message': 'Result not ready yet',
            'filename': filename
        }), 404

def process_sqs_messages():
    """后台线程函数：处理SQS响应队列消息"""
    logger.info("SQS消息处理线程启动")
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    while not shutdown_event.is_set():
        try:
            with sqs_response_lock:  # 使用sqs_response_lock
                response = sqs.receive_message(
                    QueueUrl=RESPONSE_QUEUE_URL,
                    MaxNumberOfMessages=SQS_MAX_MESSAGES,
                    WaitTimeSeconds=20,  # 长轮询
                    MessageAttributeNames=['request_id']
                )
            
            messages = response.get('Messages', [])
            if messages:
                logger.info(f"收到 {len(messages)} 条响应消息")
            
            processed_messages = []
            for message in messages:
                try:
                    # 解析消息
                    attrs = message.get('MessageAttributes', {})
                    msg_request_id = attrs.get('request_id', {}).get('StringValue')
                    
                    if not msg_request_id:
                        logger.warning("收到的消息缺少request_id属性，丢弃")
                        processed_messages.append({
                            'Id': message['MessageId'],
                            'ReceiptHandle': message['ReceiptHandle']
                        })
                        continue
                    
                    # 更新请求状态
                    body = json.loads(message['Body'])
                    result = body.get('result')
                    
                    if msg_request_id in request_records:
                        request_records[msg_request_id].update({
                            'result': result,
                            'status': 'completed'
                        })
                        logger.info(f"更新请求状态: {msg_request_id} -> completed")
                    else:
                        logger.warning(f"收到未知请求ID的响应: {msg_request_id}")
                    
                    processed_messages.append({
                        'Id': message['MessageId'],
                        'ReceiptHandle': message['ReceiptHandle']
                    })
                    
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"消息处理错误: {str(e)}，丢弃消息")
                    processed_messages.append({
                        'Id': message['MessageId'],
                        'ReceiptHandle': message['ReceiptHandle']
                    })
            
            # 批量删除已处理消息
            if processed_messages:
                with sqs_response_lock:  # 使用sqs_response_lock
                    sqs.delete_message_batch(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        Entries=processed_messages
                    )
                logger.debug(f"成功删除 {len(processed_messages)} 条SQS消息")
                
        except Exception as e:
            logger.error(f"处理SQS消息时出错: {str(e)}")
            time.sleep(5)  # 短暂暂停后重试
    
    logger.info("SQS消息处理线程停止")

def cleanup_expired_records():
    """后台线程函数：清理过期的请求记录"""
    logger.info("请求记录清理线程启动")
    
    while not shutdown_event.is_set():
        try:
            now = time.time()
            expired_ids = []
            
            # 识别过期记录
            for rid, record in request_records.items():
                if (record['status'] in ('completed', 'error', 'timeout') and
                    now - record['timestamp'] > REQUEST_TIMEOUT):
                    expired_ids.append(rid)
            
            # 删除过期记录
            for rid in expired_ids:
                request_records.pop(rid, None)
                logger.debug(f"清理过期记录: {rid}")
            
            if expired_ids:
                logger.info(f"清理了 {len(expired_ids)} 条过期记录")
                
        except Exception as e:
            logger.error(f"清理记录时出错: {str(e)}")
        
        shutdown_event.wait(CLEANUP_INTERVAL)
    
    logger.info("请求记录清理线程停止")

# 主函数
if __name__ == '__main__':

    logger.info("File: web_server.py version 2.0 release 2025-06-28 by Wenguang Zuo")
    logger.info(f"配置文件路径: {CONFIG_PATH}")

    # 启动后台线程
    sqs_processor = threading.Thread(target=process_sqs_messages, daemon=True)
    cleaner = threading.Thread(target=cleanup_expired_records, daemon=True)
    sqs_processor.start()
    cleaner.start()
    
    logger.info("Web服务器启动")
    try:
        app.run(host='0.0.0.0', port=5000, threaded=True)
    finally:
        # 优雅关闭
        shutdown_event.set()
        sqs_processor.join(timeout=10)
        cleaner.join(timeout=10)
        logger.info("Web服务器已停止")
