import sys
import pandas as pd
from datetime import datetime
import boto3
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv,
                          ['BucketName'
                           ])

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
athena_client = boto3.client(service_name='athena', region_name='us-west-2')
bucket_name=args['BucketName']
BucketName=args['BucketName']

database='c360view_stage'


sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('n1_c360_dispositions')

def run_query(client, query):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={ 'Database': database },
        ResultConfiguration={ 'OutputLocation': 's3://{}/athenaoutput/'.format(bucket_name) },
    )
    return response

def validate_query(client, query_id):
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]
    response = client.get_query_execution(QueryExecutionId=query_id)
    # wait until query finishes
    while response["QueryExecution"]["Status"]["State"] not in resp:
        response = client.get_query_execution(QueryExecutionId=query_id)
        time.sleep(2)
    return response["QueryExecution"]["Status"]["State"]

def read(query):
    print('start query: {}\n'.format(query))
    qe = run_query(athena_client, query)
    qstate = validate_query(athena_client, qe["QueryExecutionId"])
    print('query state: {}\n'.format(qstate))
    file_name = "athenaoutput/{}.csv".format(qe["QueryExecutionId"])
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(obj['Body'])

def ddl(query):
    print('start query: {}\n'.format(query))
    qe = run_query(athena_client, query)
    qstate = validate_query(athena_client, qe["QueryExecutionId"])
    print('query state: {}\n'.format(qstate))
    return qstate

def infer(location,tablename,databasename):
    # spark = SparkSession.builder.appName("spark").config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2").config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode","nonstrict").enableHiveSupport().getOrCreate()
    df = spark.read.format("parquet").parquet(location)

    cols = df.dtypes
    buf = []
    buf.append('CREATE EXTERNAL TABLE '+databasename+'.'+tablename+' ( \n')
    keyanddatatypes =  df.dtypes
    sizeof = len(df.dtypes)
    print ('size----------',sizeof)
    count=1;
    for eachvalue in keyanddatatypes:
        # print (count,sizeof,eachvalue)
        if count == sizeof:
            total = '`'+str(eachvalue[0])+str('`   ')+str(eachvalue[1])
        else:
            total =  '`'+str(eachvalue[0]) + str('`  ') + str(eachvalue[1]) + str(',')
        buf.append(total+'\n')
        count = count + 1

    buf.append(' )')
    buf.append(' STORED as parquet ')
    buf.append(" LOCATION ")
    buf.append("'")
    buf.append(location)
    buf.append("'")
    ##partition by pt
    tabledef = ''.join(buf)

    print ("---------print definition ---------")
    print (tabledef)

    ddl(tabledef)

bucket = s3.Bucket(BucketName)
bucket.objects.filter(Prefix='c360/n1_c360_dispositions/').delete()
bucket.objects.filter(Prefix='/c360/n2_c360_first_disposition/').delete()
bucket.objects.filter(Prefix='/c360/n2_c360_last_disposition/').delete()
bucket.objects.filter(Prefix='/c360/n2_c360_disposition_stats/').delete()



sql_t1="""
SELECT g.client_id,a.account_id, cd.card_id,
CASE
  WHEN cd.card_id IS NOT NULL THEN 'CARD'
  WHEN a.account_id IS NOT NULL THEN 'CHK_ACCOUNT'
END AS disp_type,
COALESCE( cd.card_id, a.account_id ) AS disposition_id,
COALESCE( cd.iss_date, a.cr_date ) AS acquisition_date
FROM c360view_stage.customer_pqt c
JOIN c360view_stage.gbank_pqt g ON c.client_id = g.client_id
LEFT JOIN c360view_stage.account_pqt a ON g.account_id = a.account_id
LEFT JOIN c360view_stage.card_pqt cd ON g.disp_id = cd.disp_id
ORDER BY acquisition_date ASC
"""

df_dispositions=read(sql_t1)


df_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")


path1= 's3://'+BucketName+'/c360/n1_c360_dispositions/'

df_spark1 = spark.createDataFrame(df_dispositions)

#df_spark1.repartition(1).write.mode("append").option("path",path1).saveAsTable("c360view_stage.n1_c360_dispositions");

df1 = DynamicFrame.fromDF(df_spark1, glueContext, "df1")

df1 = glueContext.write_dynamic_frame.from_options(
    frame = df1,
    connection_type = "s3",
    connection_options = {"path": path1},
    format = "parquet",
    transformation_ctx = "df1"
)


#n2_c360_first_disposition


df_first_dispositions = df_dispositions.groupby(["client_id"]).agg({"acquisition_date":"min","disp_type":"first","disposition_id":"first"})
df_first_dispositions = df_first_dispositions.reset_index(level=["client_id"])
df_first_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_first_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")

path2='s3://'+BucketName+'/c360/n2_c360_first_disposition/'
print(path2)

df_spark2 = spark.createDataFrame(df_first_dispositions)

df2 = DynamicFrame.fromDF(df_spark2, glueContext, "df2")

df2 = glueContext.write_dynamic_frame.from_options(
    frame = df2,
    connection_type = "s3",
    connection_options = {"path": path2},
    format = "parquet",
    transformation_ctx = "df2"
)


# N2 LAST DISPOSITIONS

df_last_dispositions = df_dispositions.groupby(["client_id"]).agg({"acquisition_date":"max","disp_type":"last","disposition_id":"last"})
df_last_dispositions = df_last_dispositions.reset_index(level=["client_id"])
df_last_dispositions['days_since_last_acq'] = df_last_dispositions.apply(lambda x: (datetime.now(tz=None) - datetime.strptime(x['acquisition_date'],'%Y-%m-%d %H:%M:%S.%f')).days ,axis=1)
df_last_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_last_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")

path3='s3://'+BucketName+'/c360/n2_c360_last_disposition/'
print(path3)

df_spark3 = spark.createDataFrame(df_first_dispositions)

df3 = DynamicFrame.fromDF(df_spark3, glueContext, "df3")

df3 = glueContext.write_dynamic_frame.from_options(
    frame = df3,
    connection_type = "s3",
    connection_options = {"path": path3},
    format = "parquet",
    transformation_ctx = "df3"
)



# N2 DISPOSITION STATS

df_dispositions['num_accounts'] = df_dispositions.groupby("client_id")['account_id'].transform("nunique")
df_dispositions['num_cards'] = df_dispositions.groupby("client_id")['card_id'].transform("nunique")

df_dispositions_stats = df_dispositions.groupby("client_id").agg({"num_accounts":"max","num_cards":"max"})
df_dispositions_stats['num_dispositions'] = df_dispositions_stats.apply(lambda x: x.num_accounts + x.num_cards, axis=1)
df_dispositions_stats['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_dispositions_stats['calc_time'] = datetime.today().strftime("%H:%M:%S")

path4='s3://'+BucketName+'/c360/n2_c360_disposition_stats/'
print(path4)

df_spark4 = spark.createDataFrame(df_dispositions_stats)

df4 = DynamicFrame.fromDF(df_spark4, glueContext, "df4")

df4 = glueContext.write_dynamic_frame.from_options(
    frame = df4,
    connection_type = "s3",
    connection_options = {"path": path4},
    format = "parquet",
    transformation_ctx = "df4"
)


infer(path1,'n1_c360_dispositions',database)
infer(path2,'n2_c360_first_disposition',database)
infer(path3,'n2_c360_last_disposition',database)
infer(path4,'n2_c360_disposition_stats',database)

job.commit()
