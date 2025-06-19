# web_layer.py - Web层服务（适配工作负载生成器的"myfile"键）
from flask import Flask, request, jsonify
import boto3
import os
import uuid
import logging
import json

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 配置AWS客户端
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# 环境变量配置
INPUT_S3_BUCKET = os.environ.get('INPUT_S3_BUCKET', 'image-recognition-input')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL', '')

@app.route('/upload', methods=['POST'])
def upload_image():
    try:
        # 检查文件是否存在（工作负载生成器使用"myfile"作为键）
        if 'myfile' not in request.files:
            return jsonify({'error': '未提供图像文件'}), 400
            
        image = request.files['myfile']
        if image.filename == '':
            return jsonify({'error': '未选择图像文件'}), 400
            
        # 生成唯一文件名（保留原始扩展名）
        ext = image.filename.split('.')[-1]
        filename = f"{uuid.uuid4()}.{ext}"
        local_path = f"/tmp/{filename}"
        image.save(local_path)
        
        # 上传到S3
        s3.upload_file(local_path, INPUT_S3_BUCKET, filename)
        os.remove(local_path)
        
        # 发送处理消息到SQS
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({'image_key': filename})
        )
        
        logger.info(f"图像 {filename} 已上传并发送处理请求")
        return jsonify({'message': '图像处理请求已接收', 'image_id': filename}), 200
        
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # 移除超时限制
    from werkzeug.serving import WSGIServer
    WSGIServer.handle_timeout = None
    app.run(host='0.0.0.0', port=80, debug=True)