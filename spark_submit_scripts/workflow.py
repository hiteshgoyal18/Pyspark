import requests,os,json,time


timely_timespan=60
adVisibility_timespan=1

def submit_daily_file(temp)
	try:
        response=requests.get('config.json')
        data = json.loads(response.content)
        base_api_url=data['baseAPIUrl']
        version=data['version']
    except Exception:
        print 'Could not able to load json file'
        traceback.print_exc()
    try: 
        fetch_response=requests.get(base_api_url+'/'+version+'/preProcessing/GetDailyPredictionFileJob?query=true')
        if(fetch_response.status_code==200):
            os.system("spark-submit /root/codes/pb_integrated_complete_daily_preprocessing.py >> /root/codes/daily.log 2>&1")
    except:
        pass
    return temp


def submit_timely_files(temp):
	try:
        response=requests.get('https://s3-ap-southeast-1.amazonaws.com/nlplive.humanindex.data/config.json')
        data = json.loads(response.content)
        base_api_url=data['baseAPIUrl']
        version=data['version']
    except Exception:
        print 'Could not able to load json file'
        traceback.print_exc()
    param_type=['dfp','ip','session_id']
    url=base_api_url+'/'+version+'/preProcessing/GetPredictionFileJob/'+ str(timely_timespan)+'/'
    for param in param_type:
        try:
            time.sleep(1)
            api_url=url+param+'?query=true'
            fetch_response=requests.get(api_url)
            if(fetch_response.status_code==200):
                os.system("spark-submit /root/codes/pb_integrated_complete_timed_preprocessing_"+param_type".py "+str(timely_timespan)+" >> /root/codes/dfp.log 2>&1")
        except:
            traceback.print_exc()
            pass
    try:
        time.sleep(1)
        fetch_response=requests.get(base_api_url+'/'+version+'/adVisibility/getAdVisibilityJob/'+str(adVisibility_timespan)+'?query=true')
        if(fetch_response.status_code==200):
            os.system("spark-submit /root/codes/adVisibility.py "+str(adVisibility_timespan)+" >> /root/codes/dfp.log 2>&1")
        time.sleep(1)
        fetch_response=requests.get(base_api_url+'/'+version+'/model/GetPredictionJob?query=true')
        if(fetch_response.status_code==200):
            os.system("spark-submit /root/codes/pb_integrated_complete_predictions.py >> /srv/codes/pred.log 2>&1")
    except:
        traceback.print_exc()
        pass
    return temp

while True:
	submit_daily_file()
	submit_timely_files()