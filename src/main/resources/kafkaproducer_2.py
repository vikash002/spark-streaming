
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import json
import random
import time
import datetime
import hashlib
import sys
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='')
# producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

def readCsvFile(fileName):
    columns = ['lpep_pickup_datetime', 'Lpep_dropoff_datetime', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude']
    df = pd.read_csv(fileName, header=None, skiprows=1, usecols = [0, 1, 2, 5, 6, 7, 8])
    return df

def convertDateStringToMilli(dateString):
    dt_obj = datetime.datetime.strptime(dateString,
                           '%Y-%m-%d %H:%M:%S').strftime('%s')
    return int(dt_obj)*1000

def getDropTime(first, second):
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    d = convertDateStringToMilli(st)
    return int(d)*1000 + (second - first)

def getPickUpTime():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    d = convertDateStringToMilli(st)
    return int(d)*1000
def sh256(ip):
    return hashlib.sha224(ip).hexdigest()[:6]

def createUserPayload(row, ip):
    data = {}
    data['userId'] = sh256(ip) + str(random.randint(0,1000000))
    data['pickUpLat'] = row[6]
    data['pickUpLon'] = row[5]
    data['dropLat'] = row[8]
    data['dropLon'] = row[7]
    data['pickUpTime'] = getPickUpTime()
    data['dropTime'] = getDropTime(convertDateStringToMilli(row[1]), convertDateStringToMilli(row[2]))
    return json.dumps(data)

def createDriverPayload1(row, ip):
    data = {}
    data['driverId'] = sh256(ip) + str(random.randint(0,1000000))
    data['locationLat'] = row[6]
    data['locationLon'] = row[5]
    data['timeStamp'] = getPickUpTime()
    return json.dumps(data)

def createDriverPayload2(row, ip):
    data = {}
    data['driverId'] = sh256(ip) + str(random.randint(0,1000000))
    data['locationLat'] = row[8]
    data['locationLon'] = row[7]
    data['timeStamp'] = getPickUpTime()
    return json.dumps(data)

def produceToKafka(value, topic):
    producer.send(topic,value)
    
def readAndProduce(filePath, ip):
    df = readCsvFile(filePath)
    count = 0
    for index, row in df.iterrows():
#         if count == 700:
#             break
        userPy = createUserPayload(row, ip)
        produceToKafka(userPy,'userTest1')
        print(userPy)
        rnd = random.randint(0,2)
        if rnd == 0:
            driverPy = createDriverPayload1(row, ip)
            produceToKafka(driverPy,'driverTest1')
            print(driverPy)
        else:
            driverPy = createDriverPayload2(row, ip)
            produceToKafka(driverPy,'driverTest1')
            print(driverPy)
        count += 1
path = '/home/ubuntu/green_tripdata_2015-01.csv'  

parser = argparse.ArgumentParser()
parser.add_argument( "--bid", help="Specify id of the box", required=True)
args= parser.parse_args()
k = args.bid
print k

while(True):
    if(k == '1'):
        readAndProduce(path, 'a')
    if(k == '2'):
        readAndProduce(path, 'b')
    if(k == '3'):
        readAndProduce(path, 'c')


# In[ ]:




# In[ ]:



