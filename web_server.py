# File: web_server.py v3.0 (优化版)
from flask import Flask, request, jsonify
import boto3
import os
import uuid
import time
import logging
import json
from werkzeug.serving import make_server

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS 配置
AWS_REGION = 'us-east-1'
INPUT_BUCKET = 'project2-input-bucket-abc'  # 替换为你的输入桶名
OUTPUT_BUCKET = 'project2-output-bucket-xyz' # 替换为你的输出桶名
REQUEST_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/request-queue' # 替换为你的请求 SQS URL
RESPONSE_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/response-queue' # 替换为你的响应 SQS URL

# 性能优化参数
MAX_ATTEMPTS = 40  # 最大尝试次数（减少）
POLLING_INTERVAL = 0.5  # 轮询间隔(秒)
SQS_MAX_MESSAGES = 10  # 每次获取的最大消息数（符合SQS限制）
INITIAL_POLLING_DELAY = 2  # 初始延迟时间（秒）

app = Flask(__name__)

# AWS 客户端
s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

@app.route('/classify', methods=['POST'])
def upload_file():
    logger.info("收到新的分类请求")
    
    if 'myfile' not in request.files:
        logger.warning("请求缺少文件部分")
        return 'No myfile part', 400
        
    file = request.files['myfile']
    if file.filename == '':
        logger.warning("请求包含空文件名")
        return 'No selected file', 400

    # 生成唯一文件名和请求ID
    filename = f"{str(uuid.uuid4())}_{file.filename}"
    request_id = str(uuid.uuid4())
    logger.info(f"生成文件名: {filename}, RequestID: {request_id}")
    
    try:
        # 上传到S3输入桶
        logger.debug(f"开始上传文件到S3: {INPUT_BUCKET}/{filename}")
        s3.upload_fileobj(file, INPUT_BUCKET, filename)
        logger.info(f"文件上传成功: {INPUT_BUCKET}/{filename}")
        
        # 发送到请求 SQS 队列
        sqs.send_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MessageBody=json.dumps({
                'filename': filename,
                'request_id': request_id
            })
        )
        logger.info(f"请求发送到SQS队列: {REQUEST_QUEUE_URL}")
        
        # 添加初始延迟（让worker有时间处理）
        logger.debug(f"等待 {INITIAL_POLLING_DELAY} 秒后开始轮询结果")
        time.sleep(INITIAL_POLLING_DELAY)
        
        # 等待处理结果
        result = wait_for_result(request_id)
        if result:
            logger.info(f"成功获取处理结果: {result}")
            return jsonify({
                'message': 'Processing completed',
                'filename': filename,
                'result': result
            }), 200
        else:
            logger.warning(f"请求处理超时: {request_id}")
            return jsonify({
                'message': 'Processing timeout',
                'filename': filename
            }), 408
            
    except Exception as e:
        logger.exception("处理请求时发生未预期错误")
        return str(e), 500

def wait_for_result(request_id):
    """等待响应队列中出现处理结果（使用消息属性优化）"""
    logger.info(f"开始等待结果，RequestID: {request_id}")
    attempts = 0
    
    # 动态轮询间隔（随着尝试次数增加而增加）
    current_polling_interval = POLLING_INTERVAL
    
    while attempts < MAX_ATTEMPTS:
        try:
            logger.debug(f"轮询响应队列 (尝试 #{attempts+1}/{MAX_ATTEMPTS})")
            
            # 接收消息，只获取request_id属性
            response = sqs.receive_message(
                QueueUrl=RESPONSE_QUEUE_URL,
                MaxNumberOfMessages=SQS_MAX_MESSAGES,
                WaitTimeSeconds=1,
                MessageAttributeNames=['request_id']  # 只获取request_id属性
            )
            
            messages = response.get('Messages', [])
            if messages:
                logger.info(f"收到 {len(messages)} 条响应消息")
                
            for message in messages:
                try:
                    # 从消息属性中获取request_id
                    attrs = message.get('MessageAttributes', {})
                    msg_request_id = attrs.get('request_id', {}).get('StringValue')
                    
                    if msg_request_id == request_id:
                        logger.info(f"找到匹配结果: {request_id}")
                        
                        # 删除消息
                        sqs.delete_message(
                            QueueUrl=RESPONSE_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logger.debug("成功删除SQS消息")
                        
                        # 解析消息体获取结果
                        body = json.loads(message['Body'])
                        return body.get('result')
                    
                    else:
                        # 对于不匹配的消息，重置可见性以便其他实例可以处理
                        logger.debug(f"忽略不匹配的RequestID: {msg_request_id}")
                        sqs.change_message_visibility(
                            QueueUrl=RESPONSE_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle'],
                            VisibilityTimeout=0  # 立即可见
                        )
                
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"消息处理错误: {str(e)}")
                    # 无效消息直接删除
                    sqs.delete_message(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
            
            # 动态调整轮询间隔（随尝试次数增加）
            current_polling_interval = POLLING_INTERVAL * (1 + attempts / 5)
            logger.debug(f"未找到结果，等待 {current_polling_interval:.2f}秒")
            time.sleep(current_polling_interval)
            attempts += 1
            
        except Exception as e:
            logger.error(f"轮询响应队列时出错: {str(e)}")
            # 增加更长的等待时间以应对临时错误
            time.sleep(current_polling_interval * 2)
            attempts += 1
    
    logger.warning(f"等待超时，RequestID: {request_id}")
    return None

@app.route('/result/<filename>', methods=['GET'])
def get_result(filename):
    logger.info(f"获取结果请求: {filename}")
    
    try:
        # 从S3输出桶获取结果
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
    
    except Exception as e:
        logger.error(f"获取结果时出错: {str(e)}")
        return jsonify({
            'message': 'Internal server error',
            'error': str(e)
        }), 500


if __name__ == '__main__':
    make_server.handle_timeout = None
    logger.info("Web服务器启动")
    app.run(host='0.0.0.0', port=5000, threaded=True)