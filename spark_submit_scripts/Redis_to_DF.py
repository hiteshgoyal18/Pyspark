


import pandas as pd
import redis
import json




redis_db = redis.StrictRedis(host="148.251.19.66", port=6379, password = 'n@$^#98&Smp^#!%&c@$' , db=0)


data = redis_db.hgetall("publisher_5140")



str_data = json.dumps(data)


abcd = json.loads(str_data)

import ast


init_df = pd.DataFrame(columns = ast.literal_eval(abcd[abcd.keys()[0]]).keys())


init_df['identifier'] = abcd.keys()



for i,j in enumerate(abcd.keys()):
    try:
        init_df['ip'][i] = ast.literal_eval(abcd[str(j)])['ip'].items()
        init_df['dfp'][i] = ast.literal_eval(abcd[str(j)])['dfp'].items()
        init_df['session_id'][i] = ast.literal_eval(abcd[str(j)])['session_id'].items()

        
    except:
        print i , j
        continue
    


def MaxLength (col_name):
    lengths= [None]*len(init_df[col_name])
    for i in range(len(init_df[col_name])):
        try:
            lengths[i] = len(init_df[col_name][i])
        except:
            continue
    return max(lengths)





tmp = init_df



for i in range(MaxLength('ip')):
    tmp[['ip{}'.format(i),'ip_count{}'.format(i)]]=tmp['ip'].apply(pd.Series)[i].apply(pd.Series)



for i in range(MaxLength('dfp')):
    tmp[['dfp{}'.format(i),'dfp_count{}'.format(i)]]=tmp['dfp'].apply(pd.Series)[i].apply(pd.Series)



for i in range(MaxLength('session_id')):
    tmp[['session_id{}'.format(i),'session_id_count{}'.format(i)]]=tmp['session_id'].apply(pd.Series)[i].apply(pd.Series)



tmp = tmp.fillna(0)





tmp = tmp.drop(['ip', 'session_id','dfp'], 1)





for x in range(MaxLength('ip')):
    
    for i,j in enumerate(tmp['ip{}'.format(x)]):
        try:
            tmp['ip{}'.format(x)][i] = ast.literal_eval(redis_db.hmget("ip_data",j)[0])['bot_prob']
        except:
            tmp['ip{}'.format(x)][i] = 0
            continue




for x in range(MaxLength('dfp')):
    
    for i,j in enumerate(tmp['dfp{}'.format(x)]):
        try:
            tmp['dfp{}'.format(x)][i] = ast.literal_eval(redis_db.hmget("dfp_data",j)[0])['bot_prob']
        except:
            tmp['dfp{}'.format(x)][i] = 0
            
            continue



for x in range(MaxLength('session_id')):
    
    for i,j in enumerate(tmp['session_id{}'.format(x)]):
        try:
            tmp['session_id{}'.format(x)][i] = ast.literal_eval(redis_db.hmget("session_id_data",j)[0])['bot_prob']
        except:
            tmp['session_id{}'.format(x)][i] = 0
            
            continue







scores = ['ip_score', 'dfp_score','session_id_score']


# In[191]:

tmp['ip_score'] =0
for x in range(MaxLength('ip')):
    tmp['ip_score{}'.format(x)] = tmp['ip{}'.format(x)] * tmp['ip_count{}'.format(x)]
    tmp['ip_score'] = tmp['ip_score']+tmp['ip_score{}'.format(x)]
            


tmp['dfp_score'] =0
for x in range(MaxLength('dfp')):
    tmp['dfp_score{}'.format(x)] = tmp['dfp{}'.format(x)] * tmp['dfp_count{}'.format(x)]
    tmp['dfp_score'] = tmp['dfp_score']+tmp['dfp_score{}'.format(x)]



tmp['session_id_score'] =0
for x in range(MaxLength('session_id')):
    tmp['session_id_score{}'.format(x)] = tmp['session_id{}'.format(x)] * tmp['session_id_count{}'.format(x)]
    tmp['session_id_score'] = tmp['session_id_score']+tmp['session_id_score{}'.format(x)]



df = tmp[scores]

df['bot'] = 1



import numpy as np
from sklearn import linear_model
from sklearn.cross_validation import train_test_split


X = df
y = df.pop('bot')
X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.3, random_state=42, stratify = y)


# In[235]:

X_train = np.array(X_train)
X_test = np.array(X_test)
y_train = np.array(y_train)
y_test = np.array(y_test)



regr = linear_model.LinearRegression(fit_intercept=False)
regr.fit(X_train, y_train)



#print('Coefficients: \n', regr.coef_)
#
#
#
#regr.predict(X_test)



from sklearn.externals import joblib
joblib.dump(regr, 'regr.pkl')



#clf = joblib.load('regr.pkl')
#
#
#
#clf.predict(X_test)



#from sklearn import svm
#LSVM = svm.LinearSVC(fit_intercept=False)
#LSVM.fit(X_train, y_train)
#
#
## In[1]:
#
#from flask import Flask
#
#
## In[ ]:
#
#app = Flask(__name__)
#if __name__ == '__main__':
#     app.run(port=8080)
#
#
## In[ ]:
#
#from flask import  jsonify
#from sklearn.externals import joblib
#app = Flask(__name__)
#@app.route('/predict', methods=['POST'])
#def predict():
#    json_ = request.json
#    query_df = pd.DataFrame(json_)
#    query = np.array(query_df)
#    prediction = clf.predict(query)
#    return jsonify({'prediction': list(prediction)})
#if __name__ == '__main__':
#    clf = joblib.load('regr.pkl')
#    app.run(port=8080)





