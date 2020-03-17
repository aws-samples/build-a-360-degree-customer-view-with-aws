import boto3
import os
import json
from datetime import datetime,timedelta
import time
import random


s3 = boto3.resource("s3")

region = os.getenv('region')
FilePath = 'account'
BucketName = os.getenv('BucketName')
csvDelimiter = os.getenv('csvDelimiter')

def getLine(i):
    x = random.randint(1,2)
    x = x*10
    y = x+50
    account_id = i
    daysago = random.randint(30,720)
    branch_id = random.randint(1,500)
    frequency = random.choice(['MI','WI','TI'])
    now = datetime.now() + timedelta(days=-daysago)
    str_now = now.isoformat()
    date_of_creation = str_now
    line = '%s,%s,%s,%s' % (account_id, branch_id, frequency, date_of_creation)
    return line



def lambda_handler(event, context):
    begining = datetime.now()
    newtime = datetime.now()
    count = 0
    arquivo=''
    filename='account'+'00'+time.strftime("%Y%m%d-%H%M%S")+'.csv'
    s3_path=FilePath+'/'+filename
    for i in range(1999):
        a = i+1
        line=getLine(a)
        newtime = datetime.now()
        newline=line
        arquivo=arquivo+newline+'\n'
    print(BucketName)
    s3.Bucket(BucketName).put_object(Key=s3_path, Body=arquivo)
    print('File '+ s3_path + ' saved.')
    print ('Processed ' + str(count) + ' items.')
