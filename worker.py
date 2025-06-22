# File: worker.py
import boto3
import os
import subprocess
import json
import time

# AWS 配置
AWS_REGION = 'us-east-1'
INPUT_BUCKET = 'project2-input-bucket-abc'
OUTPUT_BUCKET = 'project2-output-bucket-xyz'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/image-queue'

s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

def process_image(filename):
    try:
        # 下载图片
        input_path = f"/tmp/{filename}"
        s3.download_file(INPUT_BUCKET, filename, input_path)
        
        # 执行分类器
        result = subprocess.check_output(
            ['python3', '/home/ec2-user/classifier/image_classification.py', input_path],
            stderr=subprocess.STDOUT
        )
        
        # 解析结果 (格式: filename,result)
        _, classification = result.decode('utf-8').strip().split(',', 1)
        
        # 保存结果到输出桶
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=filename,
            Body=classification.encode('utf-8')
        )

        # 删除input文件
        os.remove(input_path)

        return True
        
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        return False

while True:
    try:
        # 从SQS获取消息
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                filename = message['Body']
                receipt_handle = message['ReceiptHandle']
                
                # 处理图像
                if process_image(filename):
                    # 成功处理后删除消息
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                else:
                    # 处理失败，将消息放回队列
                    sqs.change_message_visibility(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=0
                    )
        else:
            # 队列为空时暂停
            time.sleep(5)
            
    except Exception as e:
        print(f"Queue error: {str(e)}")
        time.sleep(10)