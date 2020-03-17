from __future__ import print_function
import boto3
import json
import os
import time

s3 = boto3.resource("s3")
 
region = os.getenv('region')
FilePath = 'crmapi'
BucketName = os.getenv('BucketName')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):

    invoke_response = lambda_client.invoke(FunctionName="c360viewCRMApi",
                                           InvocationType='RequestResponse'
                                           )
    response=invoke_response['Payload'].read()
    resp = json.loads(response)
    
    
    filename='crm'+'00'+time.strftime("%Y%m%d-%H%M%S")+'.json'
    arquivo=''
    for a in resp:
        arquivo=arquivo+str(a)+'\n'
    s3_path='crm/'+filename
    s3.Bucket(BucketName).put_object(Key=s3_path, Body=arquivo)
    
    
    return ('json file saved')
