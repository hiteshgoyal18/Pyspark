
# coding: utf-8

# In[40]:

from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import UserDefinedFunction,split,avg,countDistinct, when, count
from pyspark.sql.types import StringType, NumericType, IntegerType
from pyspark.ml.classification import RandomForestClassifier as RF, RandomForestClassificationModel
from pyspark.mllib.util import MLUtils
from pyspark.ml import Pipeline, PipelineModel
import re
import requests
import json
import os
import sys
import threading
import urllib2



conf = (SparkConf().setMaster("spark://jdc-user:7077").setAppName("hi_preprocessing_daily_app").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
#conf = (SparkConf().setAppName("hi_preprocessing_daily_app"))
sc = SparkContext(conf = conf)
sc.setLogLevel("Error")
sqlContext = SQLContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", '*************************')
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",'*************************')



# In[2]:

mydf=sqlContext.read.json('s3n://nlplive.hi.data/user/logs/nlp_session-www*.nlpcaptcha.in-2017051610*')


# In[3]:

mydf.registerTempTable('tdf')


# In[4]:

mouse_start=sqlContext.sql('select device_finger_print,event_type,event_value,time_stamp from tdf where (event_type="http_header" or event_type="mouse_move_data") order by time_stamp')


# In[5]:

event_array = [i.event_type for i in mouse_start.select('event_type').collect()]


# In[6]:

label = [0]*len(event_array)
for i in range (len(event_array)-1):
    if (event_array[i] == 'http_header'):
        if(event_array[i+1]!=event_array[i]):
            label[i+1] = 1


# In[7]:

l = sc.parallelize(label)
index = sc.parallelize(range(0, l.count()))
z = index.zip(l)


# In[8]:

label_df = z.toDF(('index','label'))


# In[9]:

from pyspark.sql.functions import col
def addColumnIndex(df): 
  # Create new column names
    oldColumns = df.schema.names
    newColumns = oldColumns + ["index"]

  # Add Column index
    df_indexed = df.rdd.zipWithIndex().map(lambda (row, index):                                          row + (index,)).toDF(newColumns)

    #Rename all the columns
    new_df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], 
                  newColumns[idx]), xrange(len(oldColumns)), df_indexed)   
    return new_df





# In[10]:

# Add index now...
ms_withIndex = addColumnIndex(mouse_start)


# In[11]:

#Now time to join ...
from pyspark.sql.functions import col

newone = ms_withIndex.join(label_df, ['index'],"inner")


# In[12]:

firstStringDf = newone.filter(newone['label']==1).orderBy('index')


# In[13]:

def first_grid (gridList):
    return re.split('@', gridList)[0]


# In[14]:

udf=UserDefinedFunction(lambda x :first_grid(x), StringType())


# In[15]:

test = firstStringDf.withColumn('first_grid', udf(firstStringDf.event_value))


# In[42]:

for i in range(1,17):
    test= test.withColumn(str(i),lit(i))


# In[43]:

def gridEnter(col_name,grid1):    
        if (int(col_name) == int(grid1)):
            return 1
        else:
            return 0


# In[44]:

grid_udf = UserDefinedFunction(lambda x,y :gridEnter(x,y), IntegerType())


# In[46]:

for i in range(1,17):
    test= test.withColumn(str(i),grid_udf(test['{}'.format(i)],test.first_grid))


# In[ ]:




# In[79]:

grids=[]
for i in range(1,17):
    grids.append(str(i))


# In[ ]:




# In[49]:

dfp_grids = test.select(['device_finger_print']+grids)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[96]:

dfp_grid1_freq= dfp_grids.groupBy('device_finger_print').sum().drop('sum(device_finger_print)')

dfp_grid1_freq.show()
# In[ ]:



