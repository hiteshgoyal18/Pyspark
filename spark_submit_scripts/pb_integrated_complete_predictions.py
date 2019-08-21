
# coding: utf-8

# In[1]:

from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import UserDefinedFunction,split,avg,countDistinct
from pyspark.sql.types import StringType, NumericType
import pandas as pd 
import numpy as np
import re
from pyspark.ml.classification import RandomForestClassifier as RF, RandomForestClassificationModel
from pyspark.mllib.util import MLUtils
from pyspark.ml import Pipeline, PipelineModel
import requests
import json
import os

conf = (SparkConf().setMaster("local").setAppName("pb_integrated_complete_daily_preprocessing").set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
# In[ ]:

get_predictions=[]
try:
    fetch_response=requests.get('http://pbapi.nlpcaptcha.in/v1/model/GetPredictionJob/1')
    if(fetch_response.status_code==200):
        jobs=json.loads(fetch_response.content)['jobs']
        for job in jobs:
            prediction={}
            prediction['fetch_job_id']=job['job_id']
            prediction['fetch_path']=job['Complete_path']
            prediction['model_save_path']=job['model_save_path']
            prediction['model_type']=job['type']
            get_predictions.append(prediction)                    
    else:
        print "Api Call Failed"
except:
    print "Cannot Create  prediction job Api Request"     


# In[ ]:

def redisPush(model_predictions,model_type,file_type):
    model_predictions.count()
    
    type_df=model_predictions.select(model_type).collect()    
    pred_df=model_predictions.select('prediction').collect()
    human_df=model_predictions.select('human_prob').collect()
    bot_df=model_predictions.select('bot_prob').collect()
    
    total_len = model_predictions.count()
    batch_size=5000
    add_batch_lim=0
    print total_len
    for batch in range(0,total_len,batch_size):
        #intializing redis data
        redis_data={}
        redis_data["type"]=file_type
        print model_type
        redis_data["data"]=[]    
        # creatig a batch of 5000 or maximum available
        if(total_len-batch < batch_size ):
            add_batch_lim=total_len-batch
        else:
            add_batch_lim=batch_size
        for index in range(batch,batch+add_batch_lim):
            node_data={}
            node_data[str(file_type)]=type_df[index][model_type]
            node_data["prediction"]=pred_df[index]['prediction']
            node_data["human_prob"]=human_df[index]['human_prob']
            node_data["bot_prob"]=bot_df[index]['bot_prob']
            redis_data["data"].append(node_data)
        try:
            headers = {"content-type": "application/json" }
            send_to_redis=requests.put('http://pbapi.nlpcaptcha.in/v1/modelOutput/saveModelOutput',data=json.dumps(redis_data),headers=headers)
            print send_to_redis.status_code
            print send_to_redis.content
            if(send_to_redis.status_code==200):
                print "Successfull batch save to redis"
            else:
                print "Batch save to redis unsuccessful"
        except:
            print 'Redis save for batch failed due api failure'


# In[ ]:

def calpred(prediction_fetch_path,prediction_model_save_path,prediction_model_type):
    fetch_response=requests.get('http://pbapi.nlpcaptcha.in/v1/model/loadModel')
    
    if(prediction_model_type=='dfp'):
        column_name="device_finger_print"
    if(prediction_model_type=='session_id'):
        column_name="sessionId"
    if(prediction_model_type=="ip"):
        column_name="ip"
    
    if(fetch_response.status_code==200):
        fetch_content=json.loads(fetch_response.content)
        #get version for acknowledgement purpose
        fetch_model_version=str(fetch_content[prediction_model_type]['model_version'])
        #get the complete path to fetch data
        fetch_model_path=str(fetch_content[prediction_model_type]['model_path'])
        model = PipelineModel.load(fetch_model_path)
        fetch_df=sqlContext.read.json(prediction_fetch_path)
        predictions = model.transform(fetch_df)
        
        pred = predictions.select(column_name,'probability','prediction')
        split1_udf = UserDefinedFunction(lambda value: value[0].item(), FloatType())
        split2_udf = UserDefinedFunction(lambda value: value[1].item(), FloatType())
        
        model_predictions = pred.withColumn('human_prob', split1_udf('probability')).withColumn('bot_prob', split2_udf('probability')).drop('probability')
        model_predictions=model_predictions.dropDuplicates()
        print "{}/pp_predictions_{}_{}".format(prediction_model_save_path,prediction_model_type,str(float(fetch_model_version)))
        model_predictions.write.json("{}/pp_predictions_{}_{}".format(prediction_model_save_path,prediction_model_type,str(float(fetch_model_version))),'overwrite')
        
        redisPush(model_predictions,column_name,prediction_model_type)
    else:
        print 'load model Api Call Failed'
    return (fetch_model_version)


# In[ ]:

for value in get_predictions:
    fetch_model_version=calpred(str(value['fetch_path']),str(value['model_save_path']),str(value['model_type']))
    data = {
    "job_id": str(value['fetch_job_id']),
    "filename" : "pp_predictions_{}_{}".format(str(value['model_type']),fetch_model_version)   
    }

    try:
        ack_response=requests.put('http://pbapi.nlpcaptcha.in/v1/model/acknowledgePredictionJob', data = data)
        #check if api request is successfull or not
        if(ack_response.status_code==204):
            print "Successfull Acknowledgement"

        else:
            print "Acknowledgement Unsuccessfull"
    except:
        print "Cannot create Aknowledgement request"  


# In[ ]:




# In[ ]:




# In[ ]:



