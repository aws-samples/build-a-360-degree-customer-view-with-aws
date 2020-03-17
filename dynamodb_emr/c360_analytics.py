import sys
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from datetime import datetime, timedelta
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--BucketName", help="bucket name.")
args = parser.parse_args()
if args.BucketName:
    BucketName = args.BucketName

print(BucketName)

targetPath = 's3://'+BucketName+'/c360denormalized/'

sparkSession = SparkSession.builder \
  .appName("Items") \
  .config("hive.exec.dynamic.partition.mode", "nonstrict") \
  .config("hive.exec.dynamic.partition", "true") \
  .config("hive.exec.max.dynamic.partitions.pernode","10") \
  .config("spark.dynamicAllocation.enabled","false") \
  .enableHiveSupport() \
  .getOrCreate()


dftrans = sparkSession.sql("with \
t as (SELECT c.*, \
a.branch_id, a.frequency,a.cr_date, \
gb.disp_id, gb.type, \
l3mc.amount_avg as l3mcredit_avg, l3mc.amount_count as l3mcredit_count, l3mc.calc_date||' '||l3mc.calc_time as l3m_credit_calc, \
l3md.amount_avg as l3mdebit_avg, l3mc.amount_count as l3mdebit_count, l3md.calc_date||' '||l3md.calc_time as l3m_debit_calc, \
l3mc.disp_id as l3m_cre_disp_id, \
l3md.disp_id as l3m_deb_disp_id, \
l6mc.amount_avg as l6mcredit_avg, l6mc.amount_count as l6mcredit_count, l6mc.calc_date||' '||l6mc.calc_time as l6m_credit_calc, \
l6md.amount_avg as l6mdebit_avg, l6mc.amount_count as l6mdebit_count, l6md.calc_date||' '||l6md.calc_time as l6m_debit_calc, \
l6mc.disp_id as l6m_cre_disp_id, \
l6md.disp_id as l6m_deb_disp_id, \
ga.hits as web_hits, \
ga.lastdate as web_visit_date, \
ga.search1 as web_search1, \
ga.search2 as web_search2, \
ga.mobile as web_mobile \
FROM c360view_stage.gbank_pqt as gb \
left outer join c360view_stage.account_pqt as a on gb.account_id = a.account_id \
left outer join c360view_stage.customer_pqt as c on gb.client_id = c.client_id \
left outer join (select client_id, max(disp_id) as disp_id, avg(amount_avg) as amount_avg, sum(amount_count) as amount_count, max(calc_date) as calc_date, max(calc_time) as calc_time from c360view_stage.n1_c360_trans_stats_type_l3m where type='credit' group by client_id)  as l3mc on gb.client_id = l3mc.client_id   \
left outer join (select client_id, max(disp_id) as disp_id, avg(amount_avg) as amount_avg, sum(amount_count) as amount_count, max(calc_date) as calc_date, max(calc_time) as calc_time from c360view_stage.n1_c360_trans_stats_type_l3m where type='debit' group by client_id)  as l3md on gb.client_id = l3md.client_id   \
left outer join (select client_id, max(disp_id) as disp_id, avg(amount_avg) as amount_avg, sum(amount_count) as amount_count, max(calc_date) as calc_date, max(calc_time) as calc_time from c360view_stage.n1_c360_trans_stats_type_l6m where type='credit' group by client_id)  as l6mc on gb.client_id = l6mc.client_id   \
left outer join (select client_id, max(disp_id) as disp_id, avg(amount_avg) as amount_avg, sum(amount_count) as amount_count, max(calc_date) as calc_date, max(calc_time) as calc_time from c360view_stage.n1_c360_trans_stats_type_l6m where type='debit' group by client_id)  as l6md on gb.client_id = l6md.client_id   \
left outer join (SELECT client_id, sum(t_hits) hits,max(visitdate) lastdate,max(hitpage_pagepathlevel2) search1,max(hitpage_pagepathlevel3) search2,max(device_ismobile) mobile FROM c360view_analytics.ga_sessions_analytics group by client_id) as ga on gb.client_id = ga.client_id ), \
r as (SELECT *, \
rank() OVER (PARTITION BY client_id \
                    ORDER BY branch_id,frequency DESC) AS rnk \
               FROM t \
order by client_id) \
select distinct * from r where rnk=1 \
")

#dftrans.write.mode("overwrite").parquet(targetPath)

#dftrans.createOrReplaceTempView("tempTable")

dftrans.repartition(10).write.mode("overwrite").option("path",targetPath).saveAsTable("c360view_analytics.c360denormalized");


#dftrans.show()
# |client_id|birth_date|age|home_ownership|      occupation|marital_status|head_of_household_flag| client_created_date| rn|    b_date|branch_id|frequency|             cr_date|disp_id| type|     l3mcredit_avg|l3mcredit_count|    l3m_credit_calc|      l3mdebit_avg|l3mdebit_count|     l3m_debit_calc|     l6mcredit_avg|l6mcredit_count|    l6m_credit_calc|      l6mdebit_avg|l6mdebit_count|     l6m_debit_calc|web_hits|web_visit_date|web_search1|web_search2|web_mobile|
#
