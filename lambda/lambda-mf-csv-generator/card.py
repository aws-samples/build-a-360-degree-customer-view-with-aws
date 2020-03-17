import boto3
import os
import json
from datetime import datetime,timedelta
import time
import random


s3 = boto3.resource("s3")

region = os.getenv('region')
FilePath = 'card'
BucketName = os.getenv('BucketName')
csvDelimiter = os.getenv('csvDelimiter')

def getLine():
    card_id = random.randint(5000,5999)*10000*10000*10000+random.randint(1000,1999)*10000*10000+random.randint(8000,8999)*10000+random.randint(2000,8999)
    x = random.randint(1,2)
    x = x*10
    y = x+50
    disp_id = random.randint(x,y)
    type = random.choice(['junior','classic','gold','classic','classic','gold'])
    daysago = random.randint(30,720)
    now = datetime.now() + timedelta(days=-daysago)
    str_now = now.isoformat()
    issued_datetime = str_now
    line = '%s,%s,%s,%s' % (card_id, disp_id, type, issued_datetime)
    return line



def lambda_handler(event, context):
    begining = datetime.now()
    newtime = datetime.now()
    count = 0
    arquivo=''
    filename='card'+'00'+time.strftime("%Y%m%d-%H%M%S")+'.csv'
    s3_path=FilePath+'/'+filename
    while (newtime - begining).total_seconds()<5:
        line=getLine()
        newtime = datetime.now()
        newline=line
        arquivo=arquivo+newline+'\n'
    print(BucketName)
    s3.Bucket(BucketName).put_object(Key=s3_path, Body=arquivo)
    print('File '+ s3_path + ' saved.')
    print ('Processed ' + str(count) + ' items.')
