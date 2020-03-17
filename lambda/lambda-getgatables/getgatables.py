import boto3
import os
import json
from datetime import datetime,timedelta
import time
import random

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

region = os.getenv('region')
FilePath = 'GA'
BucketName = os.getenv('BucketName')
SourceBucket = os.getenv('SourceBucket')
csvDelimiter = os.getenv('csvDelimiter')
tables = ['20170724','20170725','20170726','20170727','20170728','20170729','20170730','20170731']

def lambda_handler(event, context):
    for table in tables:
        key='data/GA/ga_sessions_' + table+'/ga_sessions_'+table+'.json'
        print(BucketName)
        print(key)
        copy_source = {
                'Bucket': SourceBucket,
                'Key': key
            }
        s3_resource.meta.client.copy(copy_source, BucketName, key)
    return('ok')
