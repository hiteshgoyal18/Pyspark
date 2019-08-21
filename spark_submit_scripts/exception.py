from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql.functions import UserDefinedFunction
import datetime
from pyspark.sql.functions import lit
from pandas.io import sql
import sys
import traceback
import requests
import json

conf = (SparkConf().setMaster("local").setAppName("hi_report_app").set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

engine = create_engine('mysql+pymysql://spark_user:AzQw4WJZtecT7xmV@148.251.19.66/nlp_live')
connection= engine.connect()

try:
	data_df=sqlContext.read.json("s3://nlplive.hi.data/logs/{nlp_session-www*.nlpcaptcha.in-2017040405*}")
	data_df.registerTempTable('myTab')
except org.apache.hadoop.mapred.InvalidInputException:
	print 's3 path does not exist.. Please check'

mydf2=sqlContext.sql("Select sessionId,device_finger_print,publisher_id,ct,id1,id2,id3,id4,id5,source,referal_url,user_agent,device,browser,ip,country,event_type,event_value,time_stamp from mytab")
def NonAsciiToAscii(user_agent):
  return user_agent.encode("ascii","ignore")

udf = UserDefinedFunction(lambda x:NonAsciiToAscii(x),StringType())
mydf2 = mydf2.withColumn('referal_url',udf(mydf2.referal_url))
mydf2 = mydf2.withColumn('user_agent',udf(mydf2.user_agent))

mydf=mydf2.toPandas()

mydf.to_sql(con=engine, name='live_hi_raw_data', if_exists='replace', index=False, chunksize=2000)
print "done..................."



