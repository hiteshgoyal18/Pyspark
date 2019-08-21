from pyspark import SparkConf,SparkContext,SQLContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import UserDefinedFunction,split
from pyspark.sql.types import StringType, NumericType
import sys
import re
import requests
import json
import threading
import traceback

try:
    conf = (SparkConf().setMaster("local[*]").setAppName("hi_daily_app"))
    sc = SparkContext(conf = conf)
    sc.setLogLevel("WARN")
    sqlContext = SQLContext(sc)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", '*********')
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",'*****************')


    try:
        mydf = sqlContext.read.json("bucket_date_range")
        mydf.registerTempTable("testtable")
        mydf.registerTempTable('calTable')
        mydf.registerTempTable('hi_events')
    except:
        traceback.print_exc()
        print 'path does not exist, Data of this timespan is not present on s3'
        # url = str(config['baseAPIUrl'])+'/'+str(config['version'])+'/preProcessing/acknowledgeDailyPredictionFileJob?isProcessed=noData'
        # headers = {"content-type": "application/json" }
        # try:
        #     ack_response = requests.put(url,data = {"job_id":fetch_job_id})
        #     if(ack_response.status_code==204):
        #         print "Successfull Acknowledgement"
        #         sys.exit(0)
        # except:
        #     print "Cannot create acknowledgement request for no data in s3"

    # In[8]:

    # find average of two columns and userdefined function from dataframe
    def findAvg(x,y):
        ip,temp,sum=x[0],y[0],0
        diff=[]
        for i, j in zip(range(len(x)),range(len(y))):
            if(x[i]!=ip):
                sum+=abs(int(y[j])-temp)
                diff.append(abs(int(y[j])-temp))
                ip=x[i]
                temp=int(y[j])
        if(len(diff)==0):
            return 0
        else:
            return sum/(1.0*len(diff))        
    udf=UserDefinedFunction(lambda x,y :findAvg(x,y), StringType())

    def fillNull(x):
        if(not x):
            return 0.0
        return x


    # In[19]:

    def dfpCal():   
        # ip change avg time interval
        dfp_ip_df=mydf.select(mydf['device_finger_print'],mydf['ip'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        dfp_ip_df.registerTempTable('dfp_ip_time_table')
        dfp_ip_df=sqlContext.sql('select device_finger_print,count(distinct(ip)) as c_ip,collect_list(ip) as ips,collect_list(time_stamp) as times from dfp_ip_time_table group by device_finger_print')
        dfp_ip_df=dfp_ip_df.filter(dfp_ip_df['c_ip']>0)
        dfp_ip_df = dfp_ip_df.withColumn('ip_change_avg_time_stamp', udf(dfp_ip_df.ips,dfp_ip_df.times))

        # for session change avg time interval

        dfp_session_df=mydf.select(mydf['device_finger_print'],mydf['sessionId'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        dfp_session_df.registerTempTable('dfp_session_time_table')
        dfp_session_df=sqlContext.sql('select device_finger_print,count(distinct(sessionId)) as c_sessionId,collect_list(sessionId) as sessionIds,collect_list(time_stamp) as times from dfp_session_time_table group by device_finger_print')
        dfp_session_df=dfp_session_df.filter(dfp_session_df['c_sessionId']>0)

        dfp_session_df = dfp_session_df.withColumn('session_change_avg_time_stamp', udf(dfp_session_df.sessionIds,dfp_session_df.times))

        # for country change avg time interval

        dfp_country_df=mydf.select(mydf['device_finger_print'],mydf['country'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        dfp_country_df.registerTempTable('dfp_country_time_table')
        dfp_country_df=sqlContext.sql('select device_finger_print,count(distinct(country)) as c_country,collect_list(country) as countrys,collect_list(time_stamp) as times from dfp_country_time_table group by device_finger_print')
        dfp_country_df=dfp_country_df.filter(dfp_country_df['c_country']>0)

        dfp_country_df = dfp_country_df.withColumn('country_change_avg_time_stamp', udf(dfp_country_df.countrys,dfp_country_df.times))


        #for impression_count and impression_time_interval_average :-

        dfp_impression_df=sqlContext.sql("Select device_finger_print,count(*) as c_impression, (max(time_stamp)-min(time_stamp))/(count(time_stamp)-1) as impression_change_avg_time_stamp from calTable where event_type in ('impression','komentbox_impression') group by device_finger_print")

        dfp_data_avg_df = dfp_ip_df.join(dfp_session_df,['device_finger_print'],"outer")
        dfp_data_avg_df = dfp_data_avg_df.join(dfp_country_df,['device_finger_print'],"outer")
        dfp_data_avg_df = dfp_data_avg_df.join(dfp_impression_df,['device_finger_print'],"outer")
        dfp_data_avg_df=dfp_data_avg_df.drop('ips')
        dfp_data_avg_df=dfp_data_avg_df.drop('times')
        dfp_data_avg_df=dfp_data_avg_df.drop('sessionIds')
        dfp_data_avg_df=dfp_data_avg_df.drop('countrys')

        udfFillNull=UserDefinedFunction(lambda x :fillNull(x), StringType())
        dfp_data_avg_df = dfp_data_avg_df.withColumn('ip_change_avg_time_stamp', udfFillNull(dfp_data_avg_df.ip_change_avg_time_stamp))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('session_change_avg_time_stamp', udfFillNull(dfp_data_avg_df.session_change_avg_time_stamp))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('country_change_avg_time_stamp', udfFillNull(dfp_data_avg_df.country_change_avg_time_stamp))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('impression_change_avg_time_stamp', udfFillNull(dfp_data_avg_df.impression_change_avg_time_stamp))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('c_ip', udfFillNull(dfp_data_avg_df.c_ip))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('c_sessionId', udfFillNull(dfp_data_avg_df.c_sessionId))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('c_country', udfFillNull(dfp_data_avg_df.c_country))
        dfp_data_avg_df = dfp_data_avg_df.withColumn('c_impression', udfFillNull(dfp_data_avg_df.c_impression))

        final_dfp_df=dfp_data_avg_df
        final_dfp_df=final_dfp_df.withColumn('ip_change_avg_time_stamp', final_dfp_df.ip_change_avg_time_stamp.cast('float').alias('ip_change_avg_time_stamp'))
        final_dfp_df=final_dfp_df.withColumn('c_ip', final_dfp_df.c_ip.cast('float').alias('c_ip'))
        final_dfp_df=final_dfp_df.withColumn('c_sessionId', final_dfp_df.c_sessionId.cast('float').alias('c_sessionId'))
        final_dfp_df=final_dfp_df.withColumn('session_change_avg_time_stamp', final_dfp_df.session_change_avg_time_stamp.cast('float').alias('session_change_avg_time_stamp'))
        final_dfp_df=final_dfp_df.withColumn('c_country', final_dfp_df.c_country.cast('float').alias('c_country'))
        final_dfp_df=final_dfp_df.withColumn('country_change_avg_time_stamp',final_dfp_df.country_change_avg_time_stamp.cast('float').alias('country_change_avg_time_stamp'))
        final_dfp_df=final_dfp_df.withColumn('c_impression', final_dfp_df.c_impression.cast('float').alias('c_impression'))
        final_dfp_df=final_dfp_df.withColumn('impression_change_avg_time_stamp', final_dfp_df.impression_change_avg_time_stamp.cast('float').alias('impression_change_avg_time_stamp'))

        final_dfp_df = final_dfp_df.dropDuplicates()

        final_dfp_df.write.mode('append').json("s3://nlplive.humanindex.data/ajay_test/ppdaily_dfp_5_june","overwrite")
        # data['file_names'].append({'file_name':"ppdaily_dfp_{}".format(fetch_job_id),"type":"dfp"})

        print "Dfp Daily is created and saved"

    def sessionCal():
        # ip change avg time interval
        session_ip_df=mydf.select(mydf['sessionId'],mydf['ip'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        session_ip_df.registerTempTable('session_ip_time_table')
        session_ip_df=sqlContext.sql('select sessionId,count(distinct(ip)) as c_ip,collect_list(ip) as ips,collect_list(time_stamp) as times from session_ip_time_table group by sessionId')
        session_ip_df=session_ip_df.filter(session_ip_df['c_ip']>0)
        session_ip_df = session_ip_df.withColumn('ip_change_avg_time_stamp', udf(session_ip_df.ips,session_ip_df.times))
        # for country change avg time interval

        session_country_df=mydf.select(mydf['sessionId'],mydf['country'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        session_country_df.registerTempTable('session_country_time_table')
        session_country_df=sqlContext.sql('select sessionId,count(distinct(country)) as c_country,collect_list(country) as countrys,collect_list(time_stamp) as times from session_country_time_table group by sessionId')
        session_country_df=session_country_df.filter(session_country_df['c_country']>0)

        session_country_df = session_country_df.withColumn('country_change_avg_time_stamp', udf(session_country_df.countrys,session_country_df.times))
        # for dfp change avg time interval

        session_session_df=mydf.select(mydf['sessionId'],mydf['device_finger_print'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        session_session_df.registerTempTable('session_dfp_time_table')
        session_session_df=sqlContext.sql('select sessionId,count(distinct(device_finger_print)) as c_dfp,collect_list(device_finger_print) as dfps,collect_list(time_stamp) as times from session_dfp_time_table group by sessionId')
        session_session_df=session_session_df.filter(session_session_df['c_dfp']>0)

        session_session_df = session_session_df.withColumn('dfp_change_avg_time_stamp', udf(session_session_df.dfps,session_session_df.times))
        #for impression_count and impression_time_interval_average :-

        session_impression_df=sqlContext.sql("Select sessionId,count(*) as impression_count, (max(time_stamp)-min(time_stamp))/(count(time_stamp)-1) as impression_time_interval_average from hi_events where event_type in ('impression','komentbox_impression') group by sessionId")
        session_data_avg_df = session_ip_df.join(session_session_df,['sessionId'],"outer")
        session_data_avg_df = session_data_avg_df.join(session_country_df,['sessionId'],"outer")
        session_data_avg_df = session_data_avg_df.join(session_impression_df,['sessionId'],"outer")
        session_data_avg_df=session_data_avg_df.drop('ips')
        session_data_avg_df=session_data_avg_df.drop('times')
        session_data_avg_df=session_data_avg_df.drop('dfps')
        session_data_avg_df=session_data_avg_df.drop('countrys')

        udfFillNull=UserDefinedFunction(lambda x :fillNull(x), StringType())
        session_data_avg_df = session_data_avg_df.withColumn('ip_change_avg_time_stamp', udfFillNull(session_data_avg_df.ip_change_avg_time_stamp))
        session_data_avg_df = session_data_avg_df.withColumn('dfp_change_avg_time_stamp', udfFillNull(session_data_avg_df.dfp_change_avg_time_stamp))
        session_data_avg_df = session_data_avg_df.withColumn('country_change_avg_time_stamp', udfFillNull(session_data_avg_df.country_change_avg_time_stamp))
        session_data_avg_df = session_data_avg_df.withColumn('impression_time_interval_average', udfFillNull(session_data_avg_df.impression_time_interval_average))
       
        session_data_avg_df = session_data_avg_df.withColumn('c_ip', udfFillNull(session_data_avg_df.c_ip))
        session_data_avg_df = session_data_avg_df.withColumn('c_dfp', udfFillNull(session_data_avg_df.c_dfp))
        session_data_avg_df = session_data_avg_df.withColumn('c_country', udfFillNull(session_data_avg_df.c_country))
        session_data_avg_df = session_data_avg_df.withColumn('impression_count', udfFillNull(session_data_avg_df.impression_count))
        
        session_sessionId_df=session_data_avg_df
        session_sessionId_df=session_sessionId_df.withColumn('ip_change_avg_time_stamp', session_sessionId_df.ip_change_avg_time_stamp.cast('float').alias('ip_change_avg_time_stamp'))
        session_sessionId_df=session_sessionId_df.withColumn('c_ip', session_sessionId_df.c_ip.cast('float').alias('c_ip'))
        session_sessionId_df=session_sessionId_df.withColumn('c_dfp', session_sessionId_df.c_dfp.cast('float').alias('c_dfp'))
        session_sessionId_df=session_sessionId_df.withColumn('dfp_change_avg_time_stamp', session_sessionId_df.dfp_change_avg_time_stamp.cast('float').alias('dfp_change_avg_time_stamp'))
        session_sessionId_df=session_sessionId_df.withColumn('c_country', session_sessionId_df.c_country.cast('float').alias('c_country'))
        session_sessionId_df=session_sessionId_df.withColumn('country_change_avg_time_stamp',session_sessionId_df.country_change_avg_time_stamp.cast('float').alias('country_change_avg_time_stamp'))
        session_sessionId_df=session_sessionId_df.withColumn('impression_count', session_sessionId_df.impression_count.cast('float').alias('impression_count'))
        session_sessionId_df=session_sessionId_df.withColumn('impression_time_interval_average', session_sessionId_df.impression_time_interval_average.cast('float').alias('impression_time_interval_average'))
       
        session_sessionId_df = session_sessionId_df.dropDuplicates()
        session_sessionId_df.write.mode('append').json("bucket_path","overwrite")
        # data['file_names'].append({'file_name':"ppdaily_session_id_{}".format(fetch_job_id),"type":"session_id"})
        print "Session Daily is created and saved"

    def IPCal():
        # session change avg time interval
        ip_ip_df=mydf.select(mydf['ip'],mydf['sessionId'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        ip_ip_df.registerTempTable('ip_ip_time_table')
        ip_ip_df=sqlContext.sql('select ip,count(distinct(sessionId)) as c_sessionId,collect_list(sessionId) as sessionIds,collect_list(time_stamp) as times from ip_ip_time_table group by ip')
        ip_ip_df=ip_ip_df.filter(ip_ip_df['c_sessionId']>0)
        ip_ip_df = ip_ip_df.withColumn('session_change_avg_time_stamp', udf(ip_ip_df.sessionIds,ip_ip_df.times))
        # for country change avg time interval

        ip_country_df=mydf.select(mydf['ip'],mydf['country'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        ip_country_df.registerTempTable('ip_country_time_table')
        ip_country_df=sqlContext.sql('select ip,count(distinct(country)) as c_country,collect_list(country) as countrys,collect_list(time_stamp) as times from ip_country_time_table group by ip')
        ip_country_df=ip_country_df.filter(ip_country_df['c_country']>0)

        ip_country_df = ip_country_df.withColumn('country_change_avg_time_stamp', udf(ip_country_df.countrys,ip_country_df.times))
        # for dfp change avg time interval

        ip_session_df=mydf.select(mydf['ip'],mydf['device_finger_print'],mydf['time_stamp']).sort(mydf.time_stamp.asc())
        ip_session_df.registerTempTable('ip_session_time_table')
        ip_session_df=sqlContext.sql('select ip,count(distinct(device_finger_print)) as c_dfp,collect_list(device_finger_print) as dfps,collect_list(time_stamp) as times from ip_session_time_table group by ip')
        ip_session_df=ip_session_df.filter(ip_session_df['c_dfp']>0)

        ip_session_df = ip_session_df.withColumn('dfp_change_avg_time_stamp', udf(ip_session_df.dfps,ip_session_df.times))
        #for impression_count and impression_time_interval_average :-

        ip_impression_df=sqlContext.sql("Select ip,count(*) as impression_count, (max(time_stamp)-min(time_stamp))/(count(time_stamp)-1) as impression_time_interval_average from hi_events where event_type in ('impression','komentbox_impression') group by ip")

        ip_data_avg_df = ip_ip_df.join(ip_session_df,['ip'],"outer")
        ip_data_avg_df = ip_data_avg_df.join(ip_country_df,['ip'],"outer")
        ip_data_avg_df = ip_data_avg_df.join(ip_impression_df,['ip'],"outer")
        ip_data_avg_df=ip_data_avg_df.drop('dfps')
        ip_data_avg_df=ip_data_avg_df.drop('times')
        ip_data_avg_df=ip_data_avg_df.drop('sessionIds')
        ip_data_avg_df=ip_data_avg_df.drop('countrys')
            
        udfFillNull=UserDefinedFunction(lambda x :fillNull(x), StringType())
        ip_data_avg_df = ip_data_avg_df.withColumn('session_change_avg_time_stamp', udfFillNull(ip_data_avg_df.session_change_avg_time_stamp))
        ip_data_avg_df = ip_data_avg_df.withColumn('dfp_change_avg_time_stamp', udfFillNull(ip_data_avg_df.dfp_change_avg_time_stamp))
        ip_data_avg_df = ip_data_avg_df.withColumn('country_change_avg_time_stamp', udfFillNull(ip_data_avg_df.country_change_avg_time_stamp))
        ip_data_avg_df = ip_data_avg_df.withColumn('impression_time_interval_average', udfFillNull(ip_data_avg_df.impression_time_interval_average))
        
        ip_data_avg_df = ip_data_avg_df.withColumn('c_sessionId', udfFillNull(ip_data_avg_df.c_sessionId))
        ip_data_avg_df = ip_data_avg_df.withColumn('c_dfp', udfFillNull(ip_data_avg_df.c_dfp))
        ip_data_avg_df = ip_data_avg_df.withColumn('c_country', udfFillNull(ip_data_avg_df.c_country))
        ip_data_avg_df = ip_data_avg_df.withColumn('impression_count', udfFillNull(ip_data_avg_df.impression_count))
        
        ip_ip_df=ip_data_avg_df
        ip_ip_df=ip_ip_df.withColumn('session_change_avg_time_stamp', ip_ip_df.session_change_avg_time_stamp.cast('float').alias('session_change_avg_time_stamp'))
        ip_ip_df=ip_ip_df.withColumn('c_sessionId', ip_ip_df.c_sessionId.cast('float').alias('c_sessionId'))
        ip_ip_df=ip_ip_df.withColumn('c_dfp', ip_ip_df.c_dfp.cast('float').alias('c_dfp'))
        ip_ip_df=ip_ip_df.withColumn('dfp_change_avg_time_stamp', ip_ip_df.dfp_change_avg_time_stamp.cast('float').alias('dfp_change_avg_time_stamp'))
        ip_ip_df=ip_ip_df.withColumn('c_country', ip_ip_df.c_country.cast('float').alias('c_country'))
        ip_ip_df=ip_ip_df.withColumn('country_change_avg_time_stamp',ip_ip_df.country_change_avg_time_stamp.cast('float').alias('country_change_avg_time_stamp'))
        ip_ip_df=ip_ip_df.withColumn('impression_count', ip_ip_df.impression_count.cast('float').alias('impression_count'))
        ip_ip_df=ip_ip_df.withColumn('impression_time_interval_average', ip_ip_df.impression_time_interval_average.cast('float').alias('impression_time_interval_average'))

        ip_ip_df = ip_ip_df.dropDuplicates()
        ip_ip_df.write.mode('append').json("bucket_path","overwrite")
        # data['file_names'].append({'file_name':"ppdaily_ip_{}".format(fetch_job_id),"type":"ip"})
        print "IP Daily is created and saved"
            
    dfp_process=threading.Thread(target=dfpCal,args=())
    session_process=threading.Thread(target=sessionCal,args=())
    ip_process=threading.Thread(target=IPCal,args=())

    dfp_process.start()
    session_process.start()
    ip_process.start()

    dfp_process.join()
    session_process.join()
    ip_process.join()
    print "All processing done"

    # url = str(config['baseAPIUrl'])+'/'+str(config['version'])+'/preProcessing/acknowledgeDailyPredictionFileJob'
    # # Create your header as required
    # headers = {"content-type": "application/json" }
    # try:
    #     ack_response = requests.put(url, data=json.dumps(data), headers=headers)
    #     #checking if aknowledgement is successfull or not
    #     if(ack_response.status_code==204):
    #         print "Successfull Acknowledgement"
    #     else:
    #         print "Acknowledgement Unsuccessfull"
    # except:
    #     print "Cannot create acknowledgement request"
# In[ ]:
except:
    traceback.print_exc()
    print 'script failed to execute'
    # url = str(config['baseAPIUrl'])+'/'+str(config['version'])+'/preProcessing/acknowledgeDailyPredictionFileJob?isProcessed=fail'
    # headers = {"content-type": "application/json" }
    # try:
    #     ack_response = requests.put(url, data = {"job_id":fetch_job_id})
    #     if(ack_response.status_code==204):
    #         print "Successfull negative Acknowledgement ===> Script Failure"
    # except:
    #     print "Cannot create acknowledgement request for script failure"    

