from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
import datetime
from pyspark.sql.functions import lit
from sqlalchemy import create_engine
import sys
import os
import traceback
import requests
import json
import time
import threading

start_time=time.time()
try:
	try:
		timespan=str(sys.argv[1])
	except IndexError:
		print "Please enter timespan as argument"
		os._exit(0)

	# spark session builder .......

	conf = SparkConf().setAppName("hi_adVisibility_app").setMaster("spark://dev-aws2:7077")
	sc = SparkContext(conf = conf)
	sqlContext = SQLContext(sc)

	try:
		response=requests.get('config.json')
		data = json.loads(response.content)
		user_name = data['mysql']['user']
		host = data['mysql']['host']
		password = data['mysql']['passwd']
		db = data['mysql']['db']
		base_api_url=data['baseAPIUrl']
		version=data['version']
	except Exception:
		print 'Could not able to load json file'
		traceback.print_exc()
		os._exit(0)

	try:
		from sqlalchemy import create_engine
		import pandas as pd
		from pandas.io import sql
	except ImportError:
		os.system("sudo pip install pymysql")
		os.system("sudo pip install sqlalchemy")
		os.system("sudo pip install pandas")
		from sqlalchemy import create_engine
		from pandas.io import sql
		import pandas as pd
		traceback.print_exc()

	url = "jdbc:mysql://"+host+"/"+db+"?user="+user_name+"&password="+password	
	# engine = create_engine('mysql+pymysql://'+user_name+':'+password+'@'+host+'/'+db+'')
	# connection= engine.connect()

	# API call to fetch file path..

	try:
		fetch_response=requests.get(base_api_url+'/'+version+'/adVisibility/getAdVisibilityJob/'+timespan)
		if(fetch_response.status_code==200):
			fetch_content=json.loads(fetch_response.content)
			final_path=str(fetch_content['complete_path'])
			fetch_job_id=int(fetch_content['job_id'])
			print fetch_content
		else:
			print 'Api Call Failed'
			print 'Status code is '+str(fetch_response.status_code)
			print 'Content is' +str(fetch_response.content)
			os._exit(0)
	except Exception:
	  	traceback.print_exc()
	  	os._exit(0)

	try:
		data_rdd=sc.textFile(final_path)
		data_df=sqlContext.read.json(data_rdd)
		data_df.registerTempTable('hi_data')
		print 'data_fetched'
	except:
		traceback.print_exc()
		print 'path does not exist, Data of this timespan is not present on s3'
		print final_path
		try:
			ack_response = requests.put(base_api_url+'/'+version+'/adVisibility/acknowledgeAdVisibilityJob?isProcessed=noData',params={"job_id":fetch_job_id})
			#checking if aknowledgement is successfull or not
			print ack_response.status_code
			if(ack_response.status_code==204):
				print "Successfull Acknowledgement"
				os._exit(0)
		except:
			traceback.print_exc()
			os._exit(0)

	# .........................summary report
	
	mydf2=sqlContext.sql("SELECT last(from_unixtime(time_stamp)) as report_date,cast(publisher_id as INT) as publisher_id,device,id1,id2,last(id3) as id3,last(id4) as id4,id5,source,country,ct,browser,SUM(IF(event_type='impression' , 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,cast(MAX(IF(event_type='ad_height',event_value,0)) as INT) as ad_height, cast(MAX(IF(event_type='ad_width',event_value,0)) as INT) as ad_width,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count FROM hi_data group by publisher_id, ct, id1, id2, id5, source, country, device, browser order by report_date ASC")
	mydf2.write.jdbc(url=url, table="nlplive_hi_raw_data1", mode="append")
	print "saved to mysql"

	#......................... identifier basis report

	mydf_web=sqlContext.sql("SELECT last(from_unixtime(time_stamp)) as report_date,id4,last(publisher_id) as publisher_id,SUM(IF(event_type='impression', 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,cast(MAX(IF(event_type='ad_height',event_value,0)) as INT) as ad_height, cast(MAX(IF(event_type='ad_width',event_value,0)) as INT) as ad_width,SUM(IF(device='mobile',1,0)) as device_mobile_count,SUM(IF(device='web',1,0)) as device_web_count,SUM(IF(device='both',1,0)) as device_other_count,SUM(IF(browser='Chrome',1,0)) as browser_chrome_count ,SUM(IF(browser='Firefox',1,0)) as browser_firefox_count,SUM(IF(browser='IE',1,0)) as browser_IE_count,SUM(IF(browser!='Firefox' AND browser!='Chrome' AND browser!='IE',1,0)) as browser_other_count,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count FROM hi_data where id4!='' group by id4 order by report_date ASC")
	mydf_web.write.jdbc(url=url, table="nlplive_hi_raw_data2", mode="append")
	print "saved to mysql"

	try:
		ack_response = requests.put(base_api_url+'/'+version+'/adVisibility/acknowledgeAdVisibilityJob',params={"job_id":fetch_job_id})
		#checking if aknowledgement is successfull or not
		print ack_response.status_code
		if(ack_response.status_code==204):
			print "Successfull Acknowledgement"
			end_time=time.time()
			print 'time taken to complete script ===> '+ str(end_time-start_time) 
		else:
			print "Acknowledgement Unsuccessfull"
			print 'Content is ' +str(ack_response.content)
			print 'Status Code is ' +str(ack_response.status_code)
	except:
		print "Cannot create Acknowledgement request"
		print 'Status Code is ' +str(ack_response.status_code)
except:
	traceback.print_exc()
	print 'script failed due to error'
	try:
		ack_response = requests.put(base_api_url+'/'+version+'/adVisibility/acknowledgeAdVisibilityJob?isProcessed=fail',params={"job_id":fetch_job_id})
		#checking if aknowledgement is successfull or not
		print ack_response.status_code
		if(ack_response.status_code==204):
			print "Successfull negative Acknowledgement ===> Script Failure"
	except:
		traceback.print_exc()
		os._exit(0)


def saveToMySQL():
	counto=0
	try:
		mydf2=sqlContext.sql("SELECT last(from_unixtime(time_stamp)) as report_date,cast(publisher_id as INT) as publisher_id,device,id1,id2,last(id3) as id3,last(id4) as id4,id5,source,country,ct,browser,SUM(IF(event_type='impression' , 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,cast(MAX(IF(event_type='ad_height',event_value,0)) as INT) as ad_height, cast(MAX(IF(event_type='ad_width',event_value,0)) as INT) as ad_width,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count FROM hi_data group by publisher_id, ct, id1, id2, id5, source, country, device, browser order by report_date ASC")
		mydf2.write.jdbc(url=url, table="nlplive_hi_raw_data1", mode="append")
		print "saved to mysql"
		counto = counto + 1

		#......................... identifier basis report

		mydf_web=sqlContext.sql("SELECT last(from_unixtime(time_stamp)) as report_date,id4,last(publisher_id) as publisher_id,SUM(IF(event_type='impression', 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,cast(MAX(IF(event_type='ad_height',event_value,0)) as INT) as ad_height, cast(MAX(IF(event_type='ad_width',event_value,0)) as INT) as ad_width,SUM(IF(device='mobile',1,0)) as device_mobile_count,SUM(IF(device='web',1,0)) as device_web_count,SUM(IF(device='both',1,0)) as device_other_count,SUM(IF(browser='Chrome',1,0)) as browser_chrome_count ,SUM(IF(browser='Firefox',1,0)) as browser_firefox_count,SUM(IF(browser='IE',1,0)) as browser_IE_count,SUM(IF(browser!='Firefox' AND browser!='Chrome' AND browser!='IE',1,0)) as browser_other_count,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count FROM hi_data where id4!='' group by id4 order by report_date ASC")
		mydf_web.write.jdbc(url=url, table="nlplive_hi_raw_data2", mode="append")
		print "saved to mysql"
		counto = counto + 1
		return counto
	except:
		counto=1
		traceback.print_exc()
		return counto

count=saveToMySQL
print count