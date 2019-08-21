import boto3
import time
from botocore.client import Config
from boto3.session import Session
start_time=time.time()

def init_aws_session(service):
  access_key_id = '******************'
  secret_access_key = '***************************'
    # Create Session
  session = Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='eu-central-1',
    )
  client = session.client(service)
  return client

s3_client = init_aws_session('s3')
# open('hello.txt').write('Hello, world!')
response = s3_client.get_bucket_tagging(
    Bucket='nlplive.hi.raw.data'
)
print response['TagSet'][0]['Key']
# Upload the file to S3
# s3_client.upload_file('part_file.json', 'nlp.hi.data', 'part_file.txt')
# print 'uploaded'
# end_time=time.time()

# print end_time-start_time

# Download the file from S3
# s3_client.download_file('MyBucket', 'hello-remote.txt', 'hello2.txt')
# print(open('hello2.txt').read())
