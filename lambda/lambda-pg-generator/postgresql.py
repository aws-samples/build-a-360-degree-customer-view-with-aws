import boto3
import random
import os
import datetime
from datetime import timedelta
import time
import psycopg2

RDS_ENDPOINT = os.getenv('RDS_ENDPOINT')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')

def getSQL():
    x = random.randint(1,2)
    x = x*10
    y = x+50
    account_id = random.randint(x,y)
    now = datetime.datetime.now()
    str_now = now.isoformat()
    date = str_now
    type = random.choice(['debit', 'credit'])
    operation = random.choice(['CCW', 'CCK', 'CAB', 'WCA', 'RAB'])
    amount = random.randint(1,100000)
    balance = random.randint(-20000,100000)
    sql = "insert into transactions values (default,%s,'%s','%s','%s',%s,%s)" % (account_id, date, type,operation,amount,balance)
    #print(sql)
    return sql


def lambda_handler(event, context):
    begining = datetime.datetime.now()
    newtime = begining
    con = psycopg2.connect(host=RDS_ENDPOINT, database='sourcemf', user='sourcemf', password=RDS_PASSWORD)
    cur = con.cursor()
    #sql = 'drop table if exists cidade'
    #cur.execute(sql)
    cur.execute('''CREATE TABLE IF NOT EXISTS transactions 
          ( trans_id serial primary key,
          account_id INT, 
          date TIMESTAMP, 
          type varchar(10), 
          operation varchar(3), 
          amount INT, 
          balance INT );''')
    #cur.execute(sql)
    #sql = "insert into transactions values (default,1,'2016-06-22 19:10:25-07','credit','CAB',10,10)"
    ##cur.execute(sql)
    con.commit()
    while (newtime - begining).total_seconds()<5:
        #result=db.customers.insert(getReferrer())
        sql=getSQL()
        newtime = datetime.datetime.now()
        cur.execute(sql)
    con.commit()
    cur.execute('select * from transactions limit 10')
    #cur.execute('select max(trans_id) from transactions')
    recset = cur.fetchall()
    print('select * from transactions limit 10')
    for rec in recset:
      print('tuple')
      print (rec)
      print('rec[2]')
      print (rec[2])
    con.close()
