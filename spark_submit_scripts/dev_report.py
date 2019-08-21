from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine
import pandas as pd
import datetime
from pyspark.sql.functions import lit
from pandas.io import sql
import re
import sys
import traceback
import requests
import json


conf = (SparkConf().setMaster("local").setAppName("hi_report_app").set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# with open('configs.json') as json_creds:
# 	data = json.load(json_creds)

# user_name=str(data['user'])
# password=str(data['passwd'])
# host=str(data['host'])
# db=str(data['db'])
# engine = create_engine('mysql+pymysql://'+user_name+':'+password+'@'+host+'/'+db+'')
# connection= engine.connect()


engine = create_engine('mysql+pymysql://spark_user:AzQw4WJZtecT7xmV@148.251.19.66/nlp_live')
connection= engine.connect()


path="s3://nlp.hi.data/logs/nlp_session-dev.nlpcaptcha.in-20170331*"
data_df=sqlContext.read.json(path)
data_df.registerTempTable('myTab')


# .........................summary report

mydf2=sqlContext.sql("SELECT publisher_id,device,id1,id2,last(id3) as id3,last(id4) as id4,id5,source,country,ct,browser,SUM(IF(event_type='impression' , 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,MAX(IF(event_type='ad_height',event_value,0)) as ad_height, MAX(IF(event_type='ad_width',event_value,0)) as ad_width,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count,last(from_unixtime(time_stamp)) as report_date FROM myTab where source!='komentbox' group by publisher_id, ct, id1, id2, id5, source, country, device, browser")


cols=mydf2.columns
cols = cols[-1:] + cols[:-1]
mydf2=mydf2[cols]
mydf=mydf2.toPandas()

mydf['report_date'] =  pd.to_datetime(mydf['report_date'])
mydf['visibility_time']=mydf['visibility_time'].astype(int)
mydf['publisher_id']=mydf['publisher_id'].astype(int)
mydf['ad_height']=mydf['ad_height'].astype(int)
mydf['ad_width']=mydf['ad_width'].astype(int)


mydf.to_sql(con=engine, name='dev_hi_report_summary', if_exists='append', index=False, chunksize=2000)
print "done..................."


#......................... identifier basis report

mydf_web=sqlContext.sql("SELECT id2,last(publisher_id) as publisher_id,SUM(IF(event_type='impression', 1,0)) as impression_count,SUM( if( event_type = 'is_blacklisted', if( event_value = 'true', 1, 0 ) , 0 ) ) AS is_blacklisted_count,MAX(IF(event_type='ad_height',event_value,0)) as ad_height, MAX(IF(event_type='ad_width',event_value,0)) as ad_width,SUM(IF(device='mobile',1,0)) as device_mobile_count,SUM(IF(device='web',1,0)) as device_web_count,SUM(IF(device='both',1,0)) as device_other_count,SUM(IF(browser='Chrome',1,0)) as browser_chrome_count ,SUM(IF(browser='Firefox',1,0)) as browser_firefox_count,SUM(IF(browser='IE',1,0)) as browser_IE_count,SUM(IF(browser!='Firefox' AND browser!='Chrome' AND browser!='IE',1,0)) as browser_other_count,MAX(IF(event_type='visibility_count',1,0)) as visibility_count,MAX(IF(event_type='visibility_time',event_value,0)) as visibility_time,if(MAX(IF(event_type='visibility_percentage',event_value,0))>0 and MAX(IF(event_type='visibility_percentage',event_value,0))<=25 and MAX(IF(event_type='visibility_percentage',event_value,0))!='',1,0) as visibility_25_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>25 and MAX(IF(event_type='visibility_percentage',event_value,0))<=50,1,0) as visibility_50_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))>50 and MAX(IF(event_type='visibility_percentage',event_value,0))<=75,1,0) as visibility_75_count,if(MAX(IF(event_type='visibility_percentage',event_value,0))!='' and MAX(IF(event_type='visibility_percentage',event_value,0))>75,1,0) as visibility_100_count,,last(from_unixtime(time_stamp)) as report_date FROM myTab where id1!='' group by id2")

cols_web=mydf_web.columns
cols_web = cols_web[-1:] + cols_web[:-1]
mydf_web=mydf_web[cols_web]
mydf_web_pandas=mydf_web.toPandas()

mydf_web_pandas['report_date']=pd.to_datetime(mydf_web_pandas['report_date'])
mydf_web_pandas['visibility_time']=mydf_web_pandas['visibility_time'].astype(int)
mydf_web_pandas['publisher_id']=mydf_web_pandas['publisher_id'].astype(int)
mydf_web_pandas['ad_height']=mydf_web_pandas['ad_height'].astype(int)
mydf_web_pandas['ad_width']=mydf_web_pandas['ad_width'].astype(int)


mydf_web_pandas.to_sql(con=engine, name='dev_hi_ad_id_report', if_exists='append', index=False, chunksize=2000)
print "done................... Report saved in mysql successfully"

