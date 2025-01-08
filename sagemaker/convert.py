import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process a CSV file from S3 and convert it to Parquet format."
    )
    parser.add_argument("--s3_bucket", required=True, help="Name of the S3 bucket")
    parser.add_argument(
        "--input_key", required=True, help="S3 key of the input CSV file"
    )
    parser.add_argument(
        "--output_prefix", required=True, help="S3 prefix for the output Parquet files"
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSV to Parquet in SageMaker").getOrCreate()

    # Parse command-line arguments
    args = parse_args()

    # S3 bucket and file paths
    bucket_name = args.s3_bucket
    input_key = args.input_key
    output_prefix = args.output_prefix

    # Build the full S3 path for the input CSV and output Parquet
    input_path = f"s3a://{bucket_name}/{input_key}"
    output_path = f"s3a://{bucket_name}/{output_prefix}"

    # Read the CSV data from S3
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = df.select(
        [col(column).alias(column.replace(" ", "_")) for column in df.columns]
    )
    # Convert the DataFrame to Parquet and partition by 'Country'
    df.write.mode("overwrite").partitionBy("Country").parquet(output_path)

    print(f"Data has been successfully processed and saved to {output_path}")
