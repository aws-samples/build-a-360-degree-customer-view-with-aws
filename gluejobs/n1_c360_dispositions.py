import sys
import awswrangler
from datetime import datetime
from awsglue.utils import getResolvedOptions

session = awswrangler.Session()
args = getResolvedOptions(sys.argv,
                          ['BucketName'
                           ])

BucketName=args['BucketName']
print(BucketName)
sql_t1="""
SELECT g.client_id,a.account_id, cd.card_id,
CASE
  WHEN cd.card_id IS NOT NULL THEN 'CARD'
  WHEN a.account_id IS NOT NULL THEN 'CHK_ACCOUNT'
END AS disp_type,
COALESCE( cd.card_id, a.account_id ) AS disposition_id,
COALESCE( cd.iss_date, a.cr_date ) AS acquisition_date
FROM customer_pqt c
JOIN gbank_pqt g ON c.client_id = g.client_id
LEFT JOIN account_pqt a ON g.account_id = a.account_id
LEFT JOIN card_pqt cd ON g.disp_id = cd.disp_id
ORDER BY acquisition_date ASC
"""

df_dispositions = session.pandas.read_sql_athena(
sql=sql_t1,
database="c360view_stage",
s3_output='s3://'+BucketName+'/athenaoutput/'
)

df_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")

path1='s3://'+BucketName+'/c360/n1_c360_dispositions/'
session.pandas.to_parquet(
dataframe=df_dispositions,
database="c360view_stage",
path=path1,
preserve_index=False
)


#n2_c360_first_disposition


df_first_dispositions = df_dispositions.groupby(["client_id"]).agg({"acquisition_date":"min","disp_type":"first","disposition_id":"first"})
df_first_dispositions = df_first_dispositions.reset_index(level=["client_id"])
df_first_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_first_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")

path2='s3://'+BucketName+'/c360/n2_c360_first_disposition/'
print(path2)

session.pandas.to_parquet(
dataframe=df_first_dispositions,
database="c360view_stage",
path=path2,
preserve_index=False
)

# N2 LAST DISPOSITIONS

df_last_dispositions = df_dispositions.groupby(["client_id"]).agg({"acquisition_date":"max","disp_type":"last","disposition_id":"last"})
df_last_dispositions = df_last_dispositions.reset_index(level=["client_id"])
df_last_dispositions['days_since_last_acq'] = df_last_dispositions.apply(lambda x: (datetime.now(tz=None) - x['acquisition_date']).days ,axis=1)
df_last_dispositions['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_last_dispositions['calc_time'] = datetime.today().strftime("%H:%M:%S")

path3='s3://'+BucketName+'/c360/n2_c360_last_disposition/'

session.pandas.to_parquet(
    dataframe=df_last_dispositions,
    database="c360view_stage",
    path=path3,
    preserve_index=False
)

# N2 DISPOSITION STATS

df_dispositions['num_accounts'] = df_dispositions.groupby("client_id")['account_id'].transform("nunique")
df_dispositions['num_cards'] = df_dispositions.groupby("client_id")['card_id'].transform("nunique")

df_dispositions_stats = df_dispositions.groupby("client_id").agg({"num_accounts":"max","num_cards":"max"})
df_dispositions_stats['num_dispositions'] = df_dispositions_stats.apply(lambda x: x.num_accounts + x.num_cards, axis=1)
df_dispositions_stats['calc_date'] = datetime.today().strftime("%Y-%m-%d")
df_dispositions_stats['calc_time'] = datetime.today().strftime("%H:%M:%S")

path4='s3://'+BucketName+'/c360/n2_c360_disposition_stats/'


session.pandas.to_parquet(
    dataframe=df_dispositions_stats,
    database="c360view_stage",
    path=path4,
    preserve_index=False
)
