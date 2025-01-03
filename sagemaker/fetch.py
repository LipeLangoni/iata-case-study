import os
import requests
import boto3
import argparse
import subprocess
output_path = '/tmp/output_data.zip'

def download(http_url):
    subprocess.run(["wget", "-O", output_path, http_url],check=True)
    
def upload(s3_bucket, s3_prefix):
    s3 = boto3.client('s3')
    s3_key = os.path.join(s3_prefix, os.path.basename(output_path))
    s3.upload_file(output_path, s3_bucket, s3_key)
    print(f"Uploaded {output_path} to s3://{s3_bucket}/{s3_key}")
            
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--http_url', type=str, required=True, help="The HTTP URL of the zip file to download")
    parser.add_argument('--s3_bucket', type=str, required=True, help="The S3 bucket where the files will be uploaded")
    parser.add_argument('--s3_prefix', type=str, required=True, help="The prefix in the S3 bucket where the files will be uploaded")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    download(args.http_url)
    upload(args.s3_bucket, args.s3_prefix)
