
import boto3
from botocore.config import Config
import os
import requests

# Create an S3 access object
s3 = boto3.client('s3')
bucket = 'greatcandidateraphael'

url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
response = requests.get(url)

s3.put_object(Bucket= bucket, Key='populationData.json', Body=response.content)