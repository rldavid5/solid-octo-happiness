import boto3
import requests
from boto3.s3.transfer import TransferConfig
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup

def lambda_handler(event, context):
    # Connect to S3
    s3 = boto3.client('s3')
    transfer_config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                      multipart_chunksize=1024 * 25, use_threads=True)

  
   # Parse the HTML file to extract the file names, URLs, and timestamps
    url = "https://download.bls.gov/pub/time.series/pr/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    find_files = soup.find("pre").text.strip().split("\n")
    links = soup.find_all("a")
    
    files_dict = {}
    for br_tag in soup.find_all("br"):
        # the timestamp is the previous sibling of the <br> tag
        time_stamp = br_tag.previous_sibling.previous_sibling
        # check if the previous sibling is a tag with the name "a"
        if br_tag.previous_sibling.name == "a":
            file_name = br_tag.previous_sibling.text.strip()
            # skip the "To Parent Directory" link
            if file_name == "[To Parent Directory]":
                continue
            files_dict[file_name] = time_stamp
    
    # Cast timestamp to timestamp object for comparison later
    for file, timestamp in files_dict.items():
        parts = timestamp.split()
        file_timestamp = parts[0] + " " + parts[1] + " " + parts[2] 
        file_datetime = datetime.strptime(file_timestamp, '%m/%d/%Y %I:%M %p')
        files_dict[file] = file_datetime
    
        
        
    # Get list of S3 files
    s3_files = s3.list_objects(Bucket='greatcandidateraphael', Prefix='files/s3/')['Contents']
    print(f"S3 bucket files: {s3_files}")

    # Download files if they don't exist in directory
    for file, timestamp in files_dict.items():
        if not any(file in s3_file['Key'] for s3_file in s3_files):
            print(f"{file} doesn't exist in the directory, downloading...")
            url = "https://download.bls.gov/pub/time.series/pr/" + file
            response = requests.get(url)
            buffer = BytesIO(response.content)
            s3.upload_fileobj(buffer, "greatcandidateraphael", f"files/s3/{file}", Config=transfer_config)
            
    # Check timestamps and replace files if necessary
    for s3_file in s3_files:
        file_name = s3_file['Key'].split("/")[-1]
        if file_name in files_dict:
            s3_timestamp = s3_file['LastModified'].strftime('%m/%d/%Y %I:%M %p')
            s3_datetime = datetime.strptime(s3_timestamp, '%m/%d/%Y %I:%M %p')
            website_datetime = files_dict[file_name]
            if s3_datetime < website_datetime:
                print(f"{file_name} has a different timestamp, downloading...")
                url = "https://download.bls.gov/pub/time.series/pr/" + file_name
                response = requests.get(url)
                buffer = BytesIO(response.content)
                s3.upload_fileobj(buffer, "greatcandidateraphael", f"files/s3/{file_name}", Config=transfer_config)
                
    # Delete files if they exist in the bucket but not on the website            
    for s3_file in s3_files:
        file_name = s3_file['Key'].split("/")[-1]
        if file_name not in files_dict:
            print(f"{file_name} doesn't exist on the website, deleting from S3...")
            s3.delete_object(Bucket='greatcandidateraphael', Key=s3_file['Key'])

    
