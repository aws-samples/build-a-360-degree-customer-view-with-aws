import boto3
import json
from datetime import datetime,timedelta,date
import time
import random



def getLine(i):
    client_id = i
    ybirth = random.randint(1930,2000)
    mbirth = random.randint(1,12)
    dbirth = random.randint(1,31)
    if mbirth == 2:
        dbirth = random.randint(1,28)
    elif mbirth == 4 or mbirth==6 or mbirth==9 or mbirth==11:
        dbirth = random.randint(1,30)
    birth_date = date(ybirth, mbirth, dbirth)
    today = date.today()
    age = today.year - birth_date.year
    if today.month < birth_date.month or today.month == birth_date.month and today.day < birth_date.day:
        age -= 1

    home_ownership = random.choice(['O','R','U'])
    occupation = random.choice([ 'accountant',	'actor',	'actress',	'air traffic controller',	'architect',	'artist',	'attorney',	'banker',	'bartender',	'barber',	'bookkeeper',	'builder',	'businessman',	'businesswoman',	'businessperson',	'butcher',	'carpenter',	'cashier',	'chef',	'coach',	'dental hygienist',	'dentist',	'designer',	'developer',	'dietician',	'doctor',	'economist',	'editor',	'electrician',	'engineer',	'farmer',	'filmmaker',	'fisherman',	'flight attendant',	'jeweler',	'judge',	'lawyer',	'mechanic',	'musician',	'nutritionist',	'nurse',	'optician',	'painter',	'pharmacist',	'photographer',	'physician',	'pilot',	'plumber',	'police officer',	'politician',	'professor',	'programmer',	'psychologist',	'receptionist',	'salesman',	'salesperson',	'saleswoman',	'secretary',	'singer'])
    marital_status = random.choice(['M','S','D'])
    head_of_household_flag  = random.choice(['Y','N'])
    daysago = random.randint(30,720)
    now = datetime.now() + timedelta(days=-daysago)
    str_now = now.isoformat()
    client_created_date  = str_now
    data = {}
    data['client_id'] = client_id
    data['birth_date'] = birth_date.isoformat()
    data['age'] = age
    data['home_ownership'] = home_ownership
    data['occupation'] = occupation
    data['marital_status'] = marital_status
    data['head_of_household_flag'] = head_of_household_flag
    data['client_created_date'] = client_created_date
    return data



def lambda_handler(event, context):
    begining = datetime.now()
    newtime = datetime.now()
    count = 0
    arquivo={}
    newline={}
    apilist=[]
    for i in range(500):
        a = i+5000
        line=getLine(a)
        newtime = datetime.now()
        newline['Record']=line
        arquivo.update(line)
        apilist.append(line)
    return(apilist)
