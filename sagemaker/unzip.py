import boto3
import zipfile
import os
from io import BytesIO

def unzip_s3_object(bucket_name, input_key, output_prefix):
    """
    Unzips a file from an S3 bucket and uploads the unzipped content back to S3.

    Parameters:
    - bucket_name: str, S3 bucket name
    - input_key: str, S3 object key of the zip file
    - output_prefix: str, S3 prefix where unzipped files will be stored
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Download the zip file into memory
    response = s3_client.get_object(Bucket=bucket_name, Key=input_key)
    zip_file_bytes = BytesIO(response['Body'].read())

    # Unzip the file in memory
    with zipfile.ZipFile(zip_file_bytes, 'r') as z:
        for file_name in z.namelist():
            if file_name.endswith('.csv'):
                # Read the file content
                file_content = z.read(file_name)

                # Define the destination key in S3
                output_key = os.path.join(output_prefix, file_name)

                # Upload the file to S3
                s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=file_content)
                print(f"Uploaded {file_name} to s3://{bucket_name}/{output_key}")
if __name__ == "__main__":
    import argparse

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Unzip an S3 object and save the contents back to S3.")
    parser.add_argument("--bucket_name", required=True, help="Name of the S3 bucket")
    parser.add_argument("--input_key", required=True, help="S3 key of the zip file")
    parser.add_argument("--output_prefix", required=True, help="S3 prefix for unzipped files")
    args = parser.parse_args()

    # Run the unzip function
    unzip_s3_object(args.bucket_name, args.input_key, args.output_prefix)
