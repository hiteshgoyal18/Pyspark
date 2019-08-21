#!/usr/bin/python
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import monotonically_increasing_id
from datetime import datetime
from pyspark.sql.functions import lit
from pandas.io import sql
import sys
import os

conf = SparkConf().setMaster("spark://94.130.12.212:7077").setAppName("hi_raw_data_app")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


engine = create_engine('mysql+pymysql://spark_user:AzQw4WJZtecT7xmV@148.251.19.66/nlp_live')
connection= engine.connect()

select_query=connection.execute("Select * from check_status where status=0 order by id ASC limit 0,1")
if(select_query.rowcount>0):
	for row in select_query:
		startTime = row[1]
		endTime = row[2]
else:
	print 'no new job..'
	os._exit(0)

connection.execute("update check_status set status=1 where Start_time='"+startTime+"' and End_Time='"+endTime+"'")
print "job allocated.....status set to 1"

filePathStartTime=datetime.strptime(startTime, '%Y-%m-%d %H').strftime('%Y%m%d%H')
filePathEndTime=datetime.strptime(endTime, '%Y-%m-%d %H').strftime('%Y%m%d%H')

loop_through=int(filePathEndTime)-int(filePathStartTime)
print loop_through
pathPrefix="s3n://nlplive.hi.data/logs/nlp_session-www*.nlpcaptcha.in-{"
timeSpan=""

for i in range(0,loop_through):
	a = int(filePathStartTime) + i
	timeSpan = timeSpan + str(a) + "*,"

completePath = pathPrefix + timeSpan.rstrip(',') + "}"

print completePath

# sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", '*************************')
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", '*************************')

myrdd=sc.textFile(completePath)
data_df=sqlContext.read.json(myrdd)
data_df.registerTempTable('hi_raw_data')

mydf2=sqlContext.sql("Select sessionId,count(*) as session_id_count,last(source) as source from hi_raw_data where source in ('captcha','komentbox','thirdparty') group by sessionId")
# def NonAsciiToAscii(user_agent):
#   return user_agent.encode("ascii","ignore")

# udf = UserDefinedFunction(lambda x:NonAsciiToAscii(x),StringType())
# mydf2 = mydf2.withColumn('referal_url',udf(mydf2.referal_url))
# mydf2 = mydf2.withColumn('user_agent',udf(mydf2.user_agent))

mydf2=mydf2.withColumn('Start_Time',lit(startTime))
mydf2=mydf2.withColumn('End_Time',lit(endTime))
# mydf2=mydf2.withColumn('id',monotonically_increasing_id())
# cols_mydf2=mydf2.columns
# cols_mydf2=cols_mydf2[-1:] + cols_mydf2[:-1]
# mydf2=mydf2[cols_mydf2]
mydf=mydf2.toPandas()

mydf.to_sql(con=engine, name='hi_raw_data', if_exists='replace', index=False, chunksize=2000)
print "done..................."

update="update check_status set status=2 where Start_Time='"+startTime+"' and End_Time='"+endTime+"'"
connection.execute(update)

print 'updated'