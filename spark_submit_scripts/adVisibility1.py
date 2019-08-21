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
	timespan=str(sys.argv[1])
except IndexError:
	print "Please enter timespan as argument"
	os._exit(0)

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