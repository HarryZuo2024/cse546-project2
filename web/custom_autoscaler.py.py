# File: custom_autoscaler.py version 2.0 release 2025-06-28 by Wenguang Zuo
# wget https://raw.githubusercontent.com/HarryZuo2024/cse546-project2/refs/heads/main/web/custom_autoscaler.py
# wget https://raw.githubusercontent.com/HarryZuo2024/cse546-project2/refs/heads/main/web/custom_autoscaler_config.json

import boto3
import time
import logging
import json
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
    CONFIG_PATH = os.environ.get('AUTOSCALER_CONFIG_PATH', r".\code\web\custom_autoscaler_config.json")
# 如果是Linux 系统
else:
    CONFIG_PATH = os.environ.get('AUTOSCALER_CONFIG_PATH', '/home/us2-user/autoscaler/custom_autoscaler.json')


# 读取配置文件
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    logger.error("未找到配置文件 custom_autoscaler_config.json")
    raise

# 启动脚本
USER_DATA = config["USER_DATA"]

# 配置参数（带默认值）
AWS_REGION = config.get("AWS_REGION", "us-east-1")
AMI_ID = config.get("AMI_ID", "ami-0e9ee3f293fffd591")
INSTANCE_TYPE = config.get("INSTANCE_TYPE", "t2.micro")
KEY_NAME = config.get("KEY_NAME", "cse546-project2-image-recognition-key")
SECURITY_GROUP_IDS = config.get("SECURITY_GROUP_IDS", ["sg-003f2d13ff90e67aa"])
IAM_ROLE_NAME = config.get("IAM_ROLE_NAME", "AppInstanceRole")
SQS_QUEUE_URL = config.get("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/257288819129/request-queue")

# 伸缩配置（带默认值）
MIN_INSTANCES = config.get("MIN_INSTANCES", 0)
MAX_INSTANCES = config.get("MAX_INSTANCES", 15)
TARGET_MESSAGES_PER_WORKER = config.get("TARGET_MESSAGES_PER_WORKER", 3)
SCALE_UP_THRESHOLD = config.get("SCALE_UP_THRESHOLD", 1)
SCALE_DOWN_THRESHOLD = config.get("SCALE_DOWN_THRESHOLD", 0)
COOLDOWN = config.get("COOLDOWN", 120)
CHECK_INTERVAL = config.get("CHECK_INTERVAL", 10)

# 标签配置（带默认值）
TAGS = config.get("TAGS", {
    "Name": "app-instance",
    "Environment": "Development",
    "Project": "CSE546-Project2",
    "ManagedBy": "CustomAutoscaler"
})

# 启动脚本（带默认值）
USER_DATA = config.get("USER_DATA", "#!/bin/bash")

# 创建AWS客户端
ec2 = boto3.client('ec2', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

# 转换标签格式为AWS API所需格式
def format_tags(tags):
    return [{'Key': k, 'Value': v} for k, v in tags.items()]

# 获取队列深度
def get_queue_depth():
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        logger.error(f"获取队列深度失败: {str(e)}")
        return 0

# 获取当前运行的实例数量
def get_running_instances():
    try:
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'image-id', 'Values': [AMI_ID]},
                {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
                {'Name': 'tag:ManagedBy', 'Values': ['CustomAutoscaler']}
            ]
        )
        
        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instances.append({
                    'InstanceId': instance['InstanceId'],
                    'LaunchTime': instance['LaunchTime'],
                    'State': instance['State']['Name']
                })
        
        return instances
    except Exception as e:
        logger.error(f"获取运行中实例失败: {str(e)}")
        return []

# 创建新的EC2实例
def create_ec2_instance():
    try:
        # 生成唯一的实例名称
        timestamp = int(time.time())
        instance_name = f"{TAGS['Name']}-{timestamp}"
        tags = TAGS.copy()
        tags['Name'] = instance_name
        
        response = ec2.run_instances(
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            KeyName=KEY_NAME,
            SecurityGroupIds=SECURITY_GROUP_IDS,
            IamInstanceProfile={'Name': IAM_ROLE_NAME},
            MinCount=1,
            MaxCount=1,
            UserData=USER_DATA,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': format_tags(tags)
            }]
        )
        
        instance_id = response['Instances'][0]['InstanceId']
        logger.info(f"创建新实例: {instance_id}")
        return instance_id
    except Exception as e:
        logger.error(f"创建实例失败: {str(e)}")
        return None

# 终止EC2实例
def terminate_instance(instance_id):
    try:
        ec2.terminate_instances(InstanceIds=[instance_id])
        logger.info(f"终止实例: {instance_id}")
        return True
    except Exception as e:
        logger.error(f"终止实例失败: {str(e)}")
        return False

# 主函数
if __name__ == '__main__':

    logger.info("File: custom_autoscaler.py version 2.0 release 2025-06-28 by Wenguang Zuo")
    logger.info(f"配置文件路径: {CONFIG_PATH}")
    logger.info("自动伸缩器启动，开始监控队列...")

    # 初始化变量
    last_scaling_time = 0
    while True:
        try:
            # 获取队列深度和当前实例数
            queue_depth = get_queue_depth()
            running_instances = get_running_instances()
            current_instance_count = len(running_instances)
            
            # 计算所需实例数
            required_instances = min(
                MAX_INSTANCES,
                max(
                    MIN_INSTANCES,
                    (queue_depth + TARGET_MESSAGES_PER_WORKER - 1) // TARGET_MESSAGES_PER_WORKER
                )
            )
            logger.debug(f"队列深度: {queue_depth}, 当前实例数: {current_instance_count}, 所需实例数: {required_instances}")    
            current_time = time.time()
            
            # 检查是否需要扩容
            if required_instances > current_instance_count and queue_depth >= SCALE_UP_THRESHOLD:
                    logger.info(f"队列深度: {queue_depth}, 当前实例数: {current_instance_count}, 所需实例数: {required_instances} -> 扩容")
                # 扩容不要等待
                # if current_time - last_scaling_time < COOLDOWN:
                #     logger.info(f"处于冷却期，跳过扩容")
                # else:
                    instances_to_add = required_instances - current_instance_count
                    logger.info(f"需要扩容，添加 {instances_to_add} 个实例")
                    
                    for _ in range(instances_to_add):
                        instance_id = create_ec2_instance()
                        if instance_id:
                            last_scaling_time = current_time
            
            
            # 检查是否需要缩容
            elif required_instances < current_instance_count and queue_depth <= SCALE_DOWN_THRESHOLD:
                logger.info(f"队列深度: {queue_depth}, 当前实例数: {current_instance_count}, 所需实例数: {required_instances} -> 缩容")
                if current_time - last_scaling_time < COOLDOWN:
                    logger.info(f"处于冷却期，跳过缩容")
                else:
                    instances_to_remove = current_instance_count - required_instances
                    logger.info(f"需要缩容，移除 {instances_to_remove} 个实例")
                    
                    # 按启动时间排序，先终止最早的实例
                    running_instances.sort(key=lambda x: x['LaunchTime'])
                    
                    for i in range(instances_to_remove):
                        instance_id = running_instances[i]['InstanceId']
                        if terminate_instance(instance_id):
                            last_scaling_time = current_time
            
            # 休眠
            time.sleep(CHECK_INTERVAL)
            
        except Exception as e:
            logger.error(f"自动伸缩器运行错误: {str(e)}")
            time.sleep(CHECK_INTERVAL)

