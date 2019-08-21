from pyspark import SparkConf,SparkContext,SQLContext
from sqlalchemy import create_engine
import boto3
import os
import pandas as pd
from pyspark.sql.types import *
from datetime import datetime
from boto3.session import Session
from pyspark.sql.functions import lit
from pandas.io import sql


conf=SparkConf().setMaster("spark://69.195.152.210:7077").setAppName("daily_preprocessing_analysis_app")
sc=SparkContext(conf=conf)
sqlContext=SQLcontext(sc)

engine=create_engine('mysql+pymysql://spark_user:AzQw4WJZtecT7xmV@148.251.19.66/nlp_live')
connection=engine.connect()

row=connection.execute("SELECT t1.date_created,t2.pf_job_id,t2.pf_name,t2.pf_name FROM daily_preprocessing_jobs t1 INNER JOIN preprocess_files t2 ON t1.job_id = t2.pf_job_id where t1.date_created='2017-06-12'")
print row