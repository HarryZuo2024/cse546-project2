# File: CustomAutoscaler.py v3.0 no user data 
import boto3
import time
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 配置参数
AWS_REGION = 'us-east-1'
AMI_ID = 'ami-09fbfb3ce82b5d252'
INSTANCE_TYPE = 't2.micro'
KEY_NAME = 'cse546-project2-image-recognition-key'
SECURITY_GROUP_IDS = ['sg-003f2d13ff90e67aa']
IAM_ROLE_NAME = 'AppInstanceRole'  # IAM 角色名称
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/257288819129/request-queue'

# 伸缩配置
MIN_INSTANCES = 0
MAX_INSTANCES = 10
TARGET_MESSAGES_PER_WORKER = 5
SCALE_UP_THRESHOLD = 1
SCALE_DOWN_THRESHOLD = 2
COOLDOWN = 120  # 冷却时间(秒)
CHECK_INTERVAL = 10  # 检查间隔(秒)

# 标签配置
TAGS = {
    'Name': 'app-instance',
    'Environment': 'Development',
    'Project': 'CSE546-Project2',
    'ManagedBy': 'CustomAutoscaler'
}

# 启动脚本
# USER_DATA = """#!/bin/bash
# """

USER_DATA = """#!/bin/bash
# # 进入项目目录
# cd /home/ec2-user/classifier
# wget https://raw.githubusercontent.com/HarryZuo2024/cse546-project2/refs/heads/main/worker.py
# cat > worker.log << EOF
# EOF
# 启动服务worker
# nohup python3 worker.py > worker.log 2>&1 &
"""

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
def main():
    last_scaling_time = 0
    
    logger.info("自动伸缩器启动，开始监控队列...")
    
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
            logger.info(f"队列深度: {queue_depth}, 当前实例数: {current_instance_count}, 所需实例数: {required_instances}")    
            current_time = time.time()
            
            # 检查是否需要扩容
            # if required_instances > current_instance_count and queue_depth >= SCALE_UP_THRESHOLD:
            if required_instances > current_instance_count:
                
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
            # elif required_instances < current_instance_count and queue_depth < SCALE_DOWN_THRESHOLD:
            elif required_instances < current_instance_count:
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

if __name__ == '__main__':
    main()