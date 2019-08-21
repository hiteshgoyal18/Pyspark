import boto3

s3=boto3.resource('s3')

print 'connected to s3'

# bucket = s3.get_bucket('nlplive.hi.data', validate=False)

print 'bukcet accessed'


for bucket in s3.buckets.all():
	name = str(bucket).split("'",1)[1]
	print name
	# for key in bucket.objects.all():
	# 	print(key.key)