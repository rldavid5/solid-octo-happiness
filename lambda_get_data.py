import requests
import boto3
from boto3.s3.transfer import TransferConfig
from io import BytesIO
from datetime import datetime

def lambda_handler(event, context):
    # Connect to S3
    s3 = boto3.client('s3')
    transfer_config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                      multipart_chunksize=1024 * 25, use_threads=True)

    # Parse the HTML file to extract the file names, URLs, and timestamps
    url = "https://download.bls.gov/pub/time.series/pr/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    file_list = soup.find("pre").text.strip().split("\n")

    for file in file_list:
        parts = file.split()
        file_timestamp = parts[3] + " " + parts[4] + " " + parts[5]
        file_datetime = datetime.strptime(file_timestamp, '%m/%d/%Y %I:%M %p')
        file_name = parts[-1].split("/")[-1]
        file_url = "https://download.bls.gov/pub/time.series/pr/" + parts[-1]

        # Check if the file already exists in the S3 bucket
        try:
            s3.head_object(Bucket='greatcandidateraphael', Key=file_name)
        except:
            # If the file does not exist, download and upload it
            file_response = requests.get(f'https://download.bls.gov/pub/time.series/pr/{file_name}')
            file_bytes = BytesIO(file_response.content)
            s3.upload_fileobj(file_bytes, 'greatcandidateraphael', file_name, Config=transfer_config)
            print(f'Uploaded new file: {file_name}')
        else:
            # If the file exists, check if it has been updated
            s3_object = s3.head_object(Bucket='my-bucket', Key=file_name)
            s3_last_modified = datetime.strptime(s3_object['LastModified'].strftime('%m/%d/%Y %I:%M %p'), '%m/%d/%Y %I:%M %p')

            if file_datetime > s3_last_modified:
                # If the file has been updated, download and upload the new version
                file_response = requests.get(f'https://download.bls.gov/pub/time.series/pr/{file_name}')
                file_bytes = BytesIO(file_response.content)
                s3.upload_fileobj(file_bytes, 'my-bucket', file_name, Config=transfer_config)
                print(f'Uploaded updated file: {file_name}')
            else:
                print(f'File up-to-date: {file_name}')