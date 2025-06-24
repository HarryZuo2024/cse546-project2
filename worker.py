# File: worker.py v4.0 s3 out save as csv file.
import boto3
import os
import subprocess
import json
import time
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS 配置
AWS_REGION = 'us-east-1'
INPUT_BUCKET = 'project2-input-bucket-abc'
OUTPUT_BUCKET = 'project2-output-bucket-xyz'
REQUEST_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/request-queue'
RESPONSE_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/response-queue'

s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

def process_image(filename):
    try:
        logger.info(f"开始处理图像: {filename}")
        
        # 下载图片
        input_path = f"/tmp/{filename}"
        s3.download_file(INPUT_BUCKET, filename, input_path)
        logger.debug(f"图片已下载到: {input_path}")
        
        # 执行分类器
        result = subprocess.check_output(
            ['python3', '/home/ec2-user/classifier/image_classification.py', input_path],
            stderr=subprocess.STDOUT
        )
        
        # 解析结果 (格式: filename,result)
        _, classification = result.decode('utf-8').strip().split(',', 1)
        logger.info(f"分类结果: {classification}")
        
        # 保存结果到输出桶
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Body=f'{filename},{classification.encode('utf-8')}',
            # 将filenam扩展名修改为.csv
            Key=os.path.splitext(filename)[0] + '.csv'
        )
        logger.debug(f"结果已保存到S3: {OUTPUT_BUCKET}/{filename}")

        # 删除input文件
        os.remove(input_path)
        logger.debug("临时文件已删除")

        return classification
        
    except subprocess.CalledProcessError as e:
        logger.error(f"分类器执行失败: {e.output.decode('utf-8')}")
        return None
    except Exception as e:
        logger.exception(f"处理图像时出错: {str(e)}")
        return None


if __name__ == '__main__':
    logger.info("Worker 启动")
    
    while True:
        try:
            # 从请求 SQS 获取消息
            logger.debug("轮询请求队列...")
            response = sqs.receive_message(
                QueueUrl=REQUEST_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                MessageAttributeNames=['All']  # 获取所有消息属性
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        body = json.loads(message['Body'])
                        filename = body.get('filename')
                        request_id = body.get('request_id')
                        receipt_handle = message['ReceiptHandle']
                        
                        # 添加消息属性到日志
                        attrs = message.get('MessageAttributes', {})
                        logger.info(f"收到新任务: {filename}, RequestID: {request_id}")
                        
                        # 处理图像
                        classification = process_image(filename)
                        if classification:
                            # 发送结果到响应队列，使用消息属性携带request_id
                            sqs.send_message(
                                QueueUrl=RESPONSE_QUEUE_URL,
                                MessageBody=json.dumps({
                                    'result': classification
                                }),
                                MessageAttributes={
                                    'request_id': {
                                        'StringValue': request_id,
                                        'DataType': 'String'
                                    }
                                }
                            )
                            logger.info(f"结果已发送到响应队列: {request_id}")
                            
                            # 成功处理后删除消息
                            sqs.delete_message(
                                QueueUrl=REQUEST_QUEUE_URL,
                                ReceiptHandle=receipt_handle
                            )
                            logger.debug("请求消息已删除")
                        else:
                            # 处理失败，将消息放回队列
                            logger.warning(f"处理失败，将消息放回队列: {filename}")
                            sqs.change_message_visibility(
                                QueueUrl=REQUEST_QUEUE_URL,
                                ReceiptHandle=receipt_handle,
                                VisibilityTimeout=0  # 立即可见
                            )
                    except json.JSONDecodeError:
                        logger.error("无效的JSON消息体")
                        # 删除无效消息
                        sqs.delete_message(
                            QueueUrl=REQUEST_QUEUE_URL,
                            ReceiptHandle=receipt_handle
                        )
                    except Exception as e:
                        logger.exception(f"处理消息时出错: {str(e)}")
            else:
                # 队列为空时暂停
                logger.debug("队列为空，等待5秒")
                time.sleep(5)
                
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"AWS服务错误: {str(e)}")
            time.sleep(10)
        except Exception as e:
            logger.exception(f"未处理的错误: {str(e)}")
            time.sleep(10)