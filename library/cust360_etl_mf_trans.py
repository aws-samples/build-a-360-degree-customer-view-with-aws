import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','BucketName'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
BucketName=args['BucketName']

path1='s3://'+BucketName+'/c360_spark/'

output_dir = path1

glueContext = GlueContext(SparkContext.getOrCreate())

#Get DynamicFrames fron GLue Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "c360view_stage", table_name = "mf_transactions_pqt", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "c360view_stage", table_name = "gbank_pqt", transformation_ctx = "datasource1")

#Convert to Dataframes
df_transactions = datasource0.toDF()
df_dispositions = datasource1.toDF()

df_dispositions = df_dispositions.drop("type")

#Filter and grouping by time windows (last 3 and 6 months) and Customer + Type of transaction Aggregation
df_trans_acc_aggr_l3m = df_transactions.filter(F.col("date") >= F.add_months(F.current_date(), -3)).groupBy("account_id","type").agg(F.avg('amount'), F.count('amount'))
df_trans_acc_aggr_l6m = df_transactions.filter(F.col("date") >= F.add_months(F.current_date(), -6)).groupBy("account_id","type").agg(F.avg('amount'), F.count('amount'))

#Join DF with customer data
df_trans_acc_aggr_l3m = df_trans_acc_aggr_l3m.join(df_dispositions,'account_id' , how="inner")
df_trans_acc_aggr_l6m = df_trans_acc_aggr_l6m.join(df_dispositions,'account_id' , how="inner")

#Renaming columns
df_trans_acc_aggr_l3m = df_trans_acc_aggr_l3m.withColumnRenamed("avg(amount)","amount_avg").withColumnRenamed("count(amount)","amount_count")
df_trans_acc_aggr_l6m = df_trans_acc_aggr_l6m.withColumnRenamed("avg(amount)","amount_avg").withColumnRenamed("count(amount)","amount_count")

#Aggregations 2
df_trans_acc_aggr_l3m = df_trans_acc_aggr_l3m.withColumnRenamed("round(avg(amount_avg), 0)","amount_avg_trans").withColumnRenamed("sum(amount_count)","count_trans")
df_trans_acc_aggr_l6m = df_trans_acc_aggr_l6m.withColumnRenamed("round(avg(amount_avg), 0)","amount_avg_trans").withColumnRenamed("sum(amount_count)","count_trans")

#Date time partition columns generation
df_trans_acc_aggr_l3m = df_trans_acc_aggr_l3m.withColumn("calc_date", lit(datetime.today().strftime("%Y-%m-%d"))).withColumn("calc_time", lit(datetime.today().strftime("%H:%M:%S")))
df_trans_acc_aggr_l6m = df_trans_acc_aggr_l6m.withColumn("calc_date", lit(datetime.today().strftime("%Y-%m-%d"))).withColumn("calc_time", lit(datetime.today().strftime("%H:%M:%S")))

#Convert it back to Glue context Dynamic frame
dyf_cust_trans_aggr_l3m = DynamicFrame.fromDF(df_trans_acc_aggr_l3m, glueContext, "dyf_cust_trans_aggr_l3m")
dyf_cust_trans_aggr_l6m = DynamicFrame.fromDF(df_trans_acc_aggr_l6m, glueContext, "dyf_cust_trans_aggr_l6m")

# Write it out Tables in Parquet


dyf_cust_trans_aggr_l3m = glueContext.write_dynamic_frame.from_options(
    frame = dyf_cust_trans_aggr_l3m,
    connection_type = "s3",
    connection_options = {"path": output_dir+"n1_c360_trans_stats_type_l3m", "partitionKeys": ["calc_date"]},
    format = "parquet",
    transformation_ctx = "dyf_cust_trans_aggr_l3m"
)

dyf_cust_trans_aggr_l6m = glueContext.write_dynamic_frame.from_options(
    frame = dyf_cust_trans_aggr_l6m,
    connection_type = "s3",
    connection_options = {"path": output_dir+"n1_c360_trans_stats_type_l6m", "partitionKeys": ["calc_date"]},
    format = "parquet",
    transformation_ctx = "dyf_cust_trans_aggr_l6m"
)

#glue_client = boto3.client('glue', region_name='us-west-2')
#glue_client.start_crawler(Name='c360_raw_crawler')

job.commit()
