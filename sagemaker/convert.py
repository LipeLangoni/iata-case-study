import argparse
import boto3
from io import BytesIO
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Unzip an S3 object and save the contents back to S3.")
    parser.add_argument("--bucket_name", required=True, help="Name of the S3 bucket")
    parser.add_argument("--input_key", required=True, help="S3 key of the zip file")
    parser.add_argument("--output_prefix", required=True, help="S3 prefix for unzipped files")
    return parser.parse_args()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

    s3 = boto3.client('s3')
    args = parse_args()

    bucket_name = args.bucket_name
    input_path = args.input_key
    output_path = args.output_prefix

    response = s3.get_object(Bucket=bucket_name, Key=input_path)
    csv_data = BytesIO(response['Body'].read())
    temp_file_path = "/tmp/temp_input.csv"
    with open(temp_file_path, "wb") as temp_file:
        temp_file.write(csv_data.getvalue())

    df = spark.read.csv(temp_file_path, header=True, inferSchema=True)
    
    parquet_output_path = "/tmp/output_data_parquet"
    
    df.write.partitionBy("Country").parquet(parquet_output_path)

    for root, dirs, files in os.walk(parquet_output_path):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = os.path.join(output_path, os.path.relpath(file_path, parquet_output_path))
            with open(file_path, "rb") as f:
                s3.put_object(Bucket=bucket_name, Key=s3_key, Body=f)
