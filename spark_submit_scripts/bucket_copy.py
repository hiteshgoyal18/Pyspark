import boto3
import datetime
import os
from boto3.session import Session


access_key_id = '*******************'
secret_access_key = '************************'
session = Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='ap-southeast-1',
    )
client = session.client('s3')
#client = boto3.client('s3')
response = client.delete_object(
    Bucket='nlplive.tag.log',
    Key='logs/events',
)

print response