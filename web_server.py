from flask import Flask, request, jsonify
import boto3
import os
import uuid
import time
# from werkzeug.serving import WSGIServer
from werkzeug.serving import make_server
from botocore.exceptions import ClientError

app = Flask(__name__)

# AWS 配置
AWS_REGION = 'us-east-1'
INPUT_BUCKET = 'project2-input-bucket-abc'  # 替换为你的输入桶名
OUTPUT_BUCKET = 'project2-output-bucket-xyz' # 替换为你的输出桶名
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/image-queue' # 替换为你的SQS URL
MAX_ATTEMPTS = 30  # 最大尝试次数
POLLING_INTERVAL = 2  # 轮询间隔(秒)

s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

@app.route('/classify', methods=['POST'])
def upload_file():
    if 'myfile' not in request.files:
        return 'No myfile part', 400
        
    file = request.files['myfile']
    if file.filename == '':
        return 'No selected file', 400

    # 生成唯一文件名
    filename = f"{str(uuid.uuid4())}_{file.filename}"
    
    try:
        # 上传到S3输入桶
        s3.upload_fileobj(file, INPUT_BUCKET, filename)
        
        # 发送到SQS队列
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=filename
        )
        
        # 等待处理结果
        result = wait_for_result(filename)
        if result:
            return jsonify({
                'message': 'Processing completed',
                'filename': filename,
                'result': result
            }), 200
        else:
            return jsonify({
                'message': 'Processing timeout',
                'filename': filename
            }), 408
            
    except Exception as e:
        return str(e), 500

def wait_for_result(filename):
    """等待S3输出桶中出现处理结果"""
    attempts = 0
    
    while attempts < MAX_ATTEMPTS:
        try:
            # 检查文件是否存在
            response = s3.get_object(Bucket=OUTPUT_BUCKET, Key=filename)
            return response['Body'].read().decode('utf-8')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # 文件不存在，继续等待
                attempts += 1
                time.sleep(POLLING_INTERVAL)
                continue
            else:
                # 其他错误，抛出异常
                raise e
    
    # 超时返回None
    return None

@app.route('/result/<filename>', methods=['GET'])
def get_result(filename):
    try:
        # 从S3输出桶获取结果
        response = s3.get_object(Bucket=OUTPUT_BUCKET, Key=filename)
        result = response['Body'].read().decode('utf-8')
        return jsonify({'filename': filename, 'result': result})
    except s3.exceptions.NoSuchKey:
        return 'Result not ready yet', 404
    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    # 移除超时限制
    # WSGIServer.handle_timeout = None
    make_server.handle_timeout = None
    app.run(host='0.0.0.0', port=5000)