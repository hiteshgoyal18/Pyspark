









#!/usr/bin/python
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
from sqlalchemy import create_engine
# import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction,monotonically_increasing_id
from datetime import datetime
from pyspark.sql.functions import lit
from pandas.io import sql
import sys
import boto3
import os

conf = SparkConf().setMaster("spark://dev-aws2:7077").setAppName("hi_raw_data_app")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# engine = create_engine('mysql+pymysql://spark_user:AzQw4WJZtecT7xmV@148.251.19.66/nlp_live')
# connection= engine.connect()

url = "jdbc:mysql://148.251.19.66/bdt_live?user=bigdata_user&password=dbphuv8qeB28JTBW"
print 'data fetch started'
myrdd=sc.textFile("s3a://nlplive.hi.raw.data/logs/nlp_session-www*.nlpcaptcha.in-{201708180[4-5]*}")
data_df=sqlContext.read.json(myrdd)
data_df.registerTempTable('hi_raw_data')
print 'data fetched'
# data_df.printSchema()
mydf2=sqlContext.sql("Select sessionId,publisher_id,device_finger_print,browserTimeStamp,ct,id1,id2,id3,id4,id5,source,browser,device,ip,country,nlpbot,event_type,event_value,referal_url,user_agent,url,token,time_stamp from hi_raw_data where ip='122.160.157.46'")
# mydf2=sqlContext.sql("Select publisher_id,ip,event_type,event_value from hi_raw_data where event_type='recall' and event_value=1").

def NonAsciiToAscii(value):
  return value.encode("ascii","ignore")

udf = UserDefinedFunction(lambda x:NonAsciiToAscii(x),StringType())
mydf2 = mydf2.withColumn('referal_url',udf(mydf2.referal_url))
mydf2 = mydf2.withColumn('user_agent',udf(mydf2.user_agent))
mydf2 = mydf2.withColumn('url',udf(mydf2.url))

# mydf=mydf2.toPandas()

# mydf.to_sql(con=engine, name='nlplive_hi_raw_data', if_exists='replace', index=True, chunksize=2000)
mydf2.write.jdbc(url=url, table="live_hi_raw_data", mode="overwrite")
print "done..................."

# update="update check_status set status=2 where Start_Time='"+startTime+"' and End_Time='"+endTime+"'"
# connection.execute(update)
