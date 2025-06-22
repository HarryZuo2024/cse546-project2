# File: worker_log.py
import boto3
import os
import subprocess
import json
import time
import logging

# 配置日志信息
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS 配置
AWS_REGION = 'us-east-1'
INPUT_BUCKET = 'project2-input-bucket-abc'
OUTPUT_BUCKET = 'project2-output-bucket-xyz'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/image-queue'

s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

def process_image(filename):
    try:
        logging.debug(f"开始处理图像: {filename}")
        # 下载图片
        input_path = f"/tmp/{filename}"
        logging.debug(f"开始从 S3 下载图像 {filename} 到 {input_path}")
        s3.download_file(INPUT_BUCKET, filename, input_path)
        logging.debug(f"图像 {filename} 下载完成")

        # 执行分类器
        logging.debug(f"开始执行图像分类器处理 {input_path}")
        result = subprocess.check_output(
            ['python3', '/home/ec2-user/classifier/image_classification.py', input_path],
            stderr=subprocess.STDOUT
        )
        logging.debug(f"图像 {filename} 分类完成，结果为: {result.decode('utf-8').strip()}")

        # 解析结果 (格式: filename,result)
        _, classification = result.decode('utf-8').strip().split(',', 1)

        # 保存结果到输出桶
        logging.debug(f"开始将图像 {filename} 的分类结果 {classification} 保存到 S3 输出桶")
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=filename,
            Body=classification.encode('utf-8')
        )
        logging.debug(f"图像 {filename} 的分类结果保存完成")

        # 删除input文件
        os.remove(input_path)
        logging.debug(f"本地临时文件 {input_path} 已删除")

        return True

    except Exception as e:
        logging.error(f"处理 {filename} 时出错: {str(e)}")
        return False

while True:
    try:
        logging.debug("开始从 SQS 队列接收消息")
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
                logging.debug(f"从 SQS 队列接收到消息，图像文件名: {filename}")

                # 处理图像
                if process_image(filename):
                    # 成功处理后删除消息
                    logging.debug(f"图像 {filename} 处理成功，开始从 SQS 队列删除消息")
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    logging.debug(f"图像 {filename} 对应的 SQS 消息已删除")
                else:
                    # 处理失败，将消息放回队列
                    logging.debug(f"图像 {filename} 处理失败，将消息放回 SQS 队列")
                    sqs.change_message_visibility(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=0
                    )
                    logging.debug(f"图像 {filename} 对应的 SQS 消息已放回队列")
        else:
            # 队列为空时暂停
            logging.debug("SQS 队列为空，暂停 5 秒")
            time.sleep(5)

    except Exception as e:
        logging.error(f"队列处理出错: {str(e)}，暂停 10 秒")
        time.sleep(10)
