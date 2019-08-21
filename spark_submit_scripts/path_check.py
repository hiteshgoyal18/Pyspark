from datetime import datetime
import time
import os
import sys


startTime=sys.argv[1]
endTime=sys.argv[2]
print startTime
print endTime

timeSpan=""

# filePathStartTime=datetime.strptime(startTime, '%Y-%m-%d %H:%M').strftime('%H')
# filePathEndTime=datetime.strptime(endTime, '%Y-%m-%d %H:%M').strftime('%H')
filePathStartTime=datetime.strptime(startTime, '%Y-%m-%d %H:%M')
filePathEndTime=datetime.strptime(endTime, '%Y-%m-%d %H:%M')
int timespan=(filePathEndTime - filePathStartTime).total_seconds()/60/60

print filePathEndTime - filePathStartTime


os._exit(0)

d1=datetime.strptime(startTime, '%Y-%m-%d %H:%M')
startTimerFrom=datetime.strptime(str(d1), '%M')
if(startTimerFrom % 10 == 0):
	print 'yes'
	os._exit(0)
d2=datetime.strptime(endTime, '%Y-%m-%d %H:%M')
minutes = (d2-d1).total_seconds()/60
print minutes
os._exit(0)



pathPrefix="s3n://nlplive.hi.raw.data/logs/nlp_session-www*.nlpcaptcha.in-{"

if(filePathStartTime[0:8]==filePathEndTime[0:8]):
	minutes = (d2-d1).total_seconds()/60
	loop_through=int(minutes/10)
	remaining=int(minutes) - loop_through
	start_digit=filePathStartTime[11:12]
	for i in range(0,loop_through):
		a = int(filePathStartTime[0:11]) + i
		timeSpan = timeSpan + str(a) + "*,"
		if(str(a)[10:11]=='5'):
			print i
			break


completePath = pathPrefix + timeSpan.rstrip(',') + "}"

print completePath
os._exit(0)

def calculate_path(loop_through,file_pattern):
	for i in range(0,loop_through):
		a = int(filePathStartTime[0:11]) + i
		timeSpan = timeSpan + str(a) + "*,"
		if(str(a)[10:11]=='5'):
			print i

