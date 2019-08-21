#!/usr/bin/python
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from boto3.session import Session
from pyspark.sql.functions import lit
from pandas.io import sql
import sys
import boto3
import os
import time
import traceback

conf = SparkConf().setMaster("spark://dev-aws2:7077").setAppName("hi_raw_data_app")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

access_key_id = '*************************'
secret_access_key = '*************************'
session = Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='eu-central-1',
    )
s3 = session.client('s3')

# get a job to execute in nlplive database....... :-
engine = create_engine('mysql+pymysql://bigdata_user:dbphuv8qeB28JTBW@148.251.19.66/bdt_live')
connection= engine.connect()

select_query=connection.execute("SELECT * FROM check_status WHERE status=0 ORDER BY job_id ASC LIMIT 0,1")
if(select_query.rowcount>0):
	for row in select_query:
		job_id=row[0]
		type_name=row[1]
		value=row[2]
		startTime = row[3]
		endTime = row[4]
		completeTimelyPath=row[6]
else:
	print 'no new job..'
	os._exit(0)

engine_pb = create_engine('mysql+pymysql://bigdata_user:dbphuv8qeB28JTBW@148.251.19.66/persistence_bridge')
# engine_pb = create_engine('mysql+pymysql://bigdata_user:YqmXzxTXyuRwUBJZ@69.195.152.210/persistence_bridge')
connection_pb= engine_pb.connect()
getDailyJob=connection_pb.execute("SELECT t1.date_created,t2.pf_job_id,t2.pf_name,t2.pf_name FROM daily_preprocessing_jobs t1 INNER JOIN preprocess_files t2 ON t1.job_id = t2.pf_job_id WHERE t1.date_created='"+str(startTime)[0:10]+"'")
daily_file_names=""
if(getDailyJob.rowcount>0):
	for i in getDailyJob:
		daily_file_names += i[2] + ","

else:
	print 'no daily job file found..'

print "Daily File names ====> "+daily_file_names
try:
	timely_file=''
	if(type_name=='sessionId'):
		getTimelyJob=connection_pb.execute("SELECT file_name,output_file_name FROM preprocessing_jobs WHERE file_start_time='"+(startTime+":00")+"' AND file_end_time='"+(endTime+":00")+"' AND type='session_id'")
		if(getTimelyJob.rowcount>0):
			for row in getTimelyJob:
				timely_file=row[0]
				summary_file_name=row[1]
				print "Timely file name is == > "+timely_file
				print "summary file name is == >"+summary_file_name

	elif(type_name=='device_finger_print'):
		getTimelyJob=connection_pb.execute("SELECT file_name,output_file_name FROM preprocessing_jobs WHERE file_start_time='"+(startTime+":00")+"' AND file_end_time='"+(endTime+":00")+"' AND type='dfp'")
		if(getTimelyJob.rowcount>0):
			for row in getTimelyJob:
				timely_file=row[0]
				summary_file_name=row[1]
				print "Timely file name is == > "+timely_file
				print "summary file name is == >"+summary_file_name

	else:
		getTimelyJob=connection_pb.execute("SELECT file_name,output_file_name FROM preprocessing_jobs WHERE file_start_time='"+(startTime+":00")+"' AND file_end_time='"+(endTime+":00")+"' AND type='ip'")
		if(getTimelyJob.rowcount>0):
			for row in getTimelyJob:
				timely_file=row[0]
				summary_file_name=row[1]
				print "Timely file name is == > "+timely_file
				print "summary file name is == >"+summary_file_name
except:
	traceback.print_exc()
	print 'no timely file found'
	pass
connection.execute("UPDATE check_status SET status=1 WHERE job_id="+str(job_id))
print type_name+" job allocated.....status set to 1"

filePathStartTime=datetime.strptime(startTime, '%Y-%m-%d %H:%M').strftime('%Y%m%d%H%M')
filePathEndTime=datetime.strptime(endTime, '%Y-%m-%d %H:%M').strftime('%Y%m%d%H%M')

# loop_through=int(filePathEndTime)-int(filePathStartTime)
# print loop_through
# pathPrefix="s3a://nlplive.hi.raw.data/logs/nlp_session-www*.nlpcaptcha.in-{"
# timeSpan=""

# for i in range(0,loop_through):
# 	a = int(filePathStartTime) + i
# 	timeSpan = timeSpan + str(a) + "*,"

# completeTimelyPath = pathPrefix + timeSpan.rstrip(',') + "}"
# print "Timed Raw Data Path ===> "+completeTimelyPath

completeDailyPath="s3a://nlplive.hi.raw.data/logs/nlp_session-www*.nlpcaptcha.in-{"+str(filePathStartTime[0:8])+"*}"
print "Full Day Raw Data Path ===> "+completeDailyPath

def saveTos3(job_id,type_name,value,completePath,type):
	myrdd=sc.textFile(completePath)
	data_df=sqlContext.read.json(myrdd)
	data_df.registerTempTable('hi_raw_data')
	if(type_name=='sessionId'):
		mydf2=sqlContext.sql("Select * from hi_raw_data where sessionId='"+value+"'")
	elif(type_name=='device_finger_print'):
		mydf2=sqlContext.sql("Select  * from hi_raw_data where device_finger_print='"+value+"'")
	else:
		mydf2=sqlContext.sql("Select * from hi_raw_data where ip='"+value+"'")

	file_name=type_name+"_"+str(job_id)+".html"
	bucket_name='nlplive.humanityindex.data'
	r=mydf2.collect()
	target=open(file_name,'w')
	for row in r:
		target.write(str(row))
		target.write("<br><br>")
	target.close()
	with open(file_name) as data:
		response=s3.put_object(ACL='public-read',Bucket=bucket_name,Key='analysis/'+type+'/pp_analysis_'+type_name+'/'+file_name, Body=data, ContentType='text/html')
	bucket_location = s3.get_bucket_location(Bucket=bucket_name)
	object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
	    	bucket_location['LocationConstraint'],
	    	bucket_name,
	    	'analysis/'+type+'/pp_analysis_'+type_name+'/'+file_name)
	print "saved to s3 ..... - > "+str(object_url)
	os.system("sudo rm "+file_name)
	print 'file deleted from local logs'
	return str(object_url)

def saveToMysql(startTime,endTime,job_id,type_name,value,daily_file_names,timely_file_name,summary_file_name):
	if('daily' in summary_file_name):
		summary_file_path="s3a://nlplive.humanityindex.data/preprocessed-data/daily/"+summary_file_name+"/part*"
	else:
		summary_file_path="s3a://nlplive.humanityindex.data/preprocessed-data/timely/"+summary_file_name+"/part*"
	print "summary file path is === > "+summary_file_path
	try:
		if(type_name=='sessionId'):
			# raw_df=sqlContext.sql("Select last(publisher_id) as publisher_id,last(sessionId) as value,count(*) as value_count,source from hi_raw_data where sessionId='"+value+"' group by source")
			daily_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/daily/"+daily_file_names.rstrip(',').split(',')[2]+"/part*")
			daily_df.registerTempTable('daily_data')
			daily_df=sqlContext.sql("Select sessionId,c_ip as ip_count,ip_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp from daily_data where sessionId='"+value+"'")
			daily_df=daily_df.withColumn('type',lit(type_name))

			summary_df=sqlContext.read.json(summary_file_path)
			summary_df.registerTempTable('summary_data')
			summary_df=sqlContext.sql("Select ession_id as sessionId,c_ip as ip_count,ip_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp from summary_data where ession_id='"+value+"'")
			summary_df=summary_df.withColumn('type',lit(type_name))

			if(timely_file_name != ''):
				timely_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/timely/"+timely_file_name+"/part*")
				timely_df.registerTempTable('timely_data')
				timely_df=sqlContext.sql("Select ession_id as sessionId,c_ip as ip_count,ip_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp from timely_data where ession_id='"+value+"'")
				timely_df=timely_df.withColumn('type',lit(type_name))
		
		elif(type_name=='device_finger_print'):
			# raw_df=sqlContext.sql("Select last(publisher_id) as publisher_id,last(device_finger_print) as value,count(*) as value_count,source from hi_raw_data where device_finger_print='"+value+"' group by source")
			daily_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/daily/"+daily_file_names.rstrip(',').split(',')[0]+"/part*")
			daily_df.registerTempTable('daily_data')
			daily_df=sqlContext.sql("Select device_finger_print,c_ip as ip_count,ip_change_avg_time_stamp,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from daily_data where device_finger_print='"+value+"'")
			daily_df=daily_df.withColumn('type',lit(type_name))

			summary_df=sqlContext.read.json(summary_file_path)
			summary_df.registerTempTable('summary_data')
			summary_df=sqlContext.sql("Select device_finger_print,c_ip as ip_count,ip_change_avg_time_stamp,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from summary_data where device_finger_print='"+value+"'")
			summary_df=summary_df.withColumn('type',lit(type_name))

			if(timely_file_name != ''):
				timely_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/timely/"+timely_file_name+"/part*")
				timely_df.registerTempTable('timely_data')
				timely_df=sqlContext.sql("Select device_finger_print,c_ip as ip_count,ip_change_avg_time_stamp,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_country as country_count,country_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from timely_data where device_finger_print='"+value+"'")
				timely_df=timely_df.withColumn('type',lit(type_name))
		
		else:
			# raw_df=sqlContext.sql("Select last(publisher_id) as publisher_id,last(ip) as value,count(*) as value_count,source from hi_raw_data where ip='"+value+"' group by source")
			daily_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/daily/"+daily_file_names.rstrip(',').split(',')[1]+"/part*")
			daily_df.registerTempTable('daily_data')
			daily_df=sqlContext.sql("Select ip,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from daily_data where ip='"+value+"'")
			daily_df=daily_df.withColumn('type',lit(type_name))

			summary_df=sqlContext.read.json(summary_file_path)
			summary_df.registerTempTable('summary_data')
			summary_df=sqlContext.sql("Select Select ip,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from summary_data where ip='"+value+"'")
			summary_df=summary_df.withColumn('type',lit(type_name))

			if(timely_file_name != ''):
				timely_df=sqlContext.read.json("s3a://nlplive.humanityindex.data/preprocessed-data/timely/"+timely_file_name+"/part*")
				timely_df.registerTempTable('timely_data')
				timely_df=sqlContext.sql("Select Select ip,c_session_id as sessionId_count,session_id_change_avg_time_stamp,c_dfp as dfp_count,dfp_change_avg_time_stamp,c_impression as impression_count,impression_change_avg_time_stamp,captcha_accuracy,empty_referer_ratio,image_percentage_ratio,page_avg_time_spent,pgs_visit_devn from timely_data where ip='"+value+"'")
				timely_df=timely_df.withColumn('type',lit(type_name))

		# raw_df=raw_df.withColumn('type',lit(type_name))
		# raw_df=raw_df.withColumn('Start_Time',lit(startTime))
		# raw_df=raw_df.withColumn('End_Time',lit(endTime))

		daily_df=daily_df.withColumn('Start_Time',lit(startTime))
		daily_df=daily_df.withColumn('End_Time',lit(endTime))
		daily_df=daily_df.withColumn('file_type',lit('daily'))

		# raw_pandas=raw_df.toPandas()
		# print 'raw_dataframe coverted to pandas'
		# raw_pandas.to_sql(con=engine, name='hi_raw_data', if_exists='append', index=False, chunksize=2000)
		# print "Raw dataframe saved in mysql"

		daily_pandas=daily_df.toPandas()
		print 'daily dataframe converted to pandas'
		daily_pandas.to_sql(con=engine, name='hi_preprocessed_data_'+type_name, if_exists='append',index=False, chunksize=2000)
		print 'Daily dataframe save to mysql'

		if(timely_file_name != ''):
			timely_df=timely_df.withColumn('Start_Time',lit(startTime))
			timely_df=timely_df.withColumn('End_Time',lit(endTime))
			timely_df=timely_df.withColumn('file_type',lit('timely'))

			timely_pandas=timely_df.toPandas()
			print 'timely dataframe converted to pandas'
			timely_pandas.to_sql(con=engine, name='hi_preprocessed_data_'+type_name, if_exists='append',index=False, chunksize=2000)
			print 'timely dataframe save to mysql'

		if(int(summary_df.count()) != 0):
			summary_df=summary_df.withColumn('Start_Time',lit(startTime))
			summary_df=summary_df.withColumn('End_Time',lit(endTime))
			summary_df=summary_df.withColumn('file_type',lit('summary'))

			summary_pandas=summary_df.toPandas()
			print 'summary dataframe converted to pandas'
			summary_pandas.to_sql(con=engine, name='hi_preprocessed_data_'+type_name, if_exists='append',index=False, chunksize=2000)
			print 'summary dataframe save to mysql'

	except:
		traceback.print_exc()
		pass
saveToMysql(startTime,endTime,job_id,type_name,value,daily_file_names,timely_file,summary_file_name)
save_daily_path=saveTos3(job_id,type_name,value,completeDailyPath,'Daily')
save_timely_path=saveTos3(job_id,type_name,value,completeTimelyPath,'Timely')

print "daily save path is ---> "+save_daily_path
print "timely save path is ---> "+save_timely_path
# update save path and status of allocated job in DB.
connection.execute("UPDATE check_status SET status=2,save_daily_path='"+save_daily_path+"',save_timely_path='"+save_timely_path+"' WHERE job_id="+str(job_id))
print 'updated'