import boto3
from botocore.exceptions import ClientError


def create_glue_database_and_table(
    database_name, table_name, s3_path, role, region_name="us-east-1"
):
    """
    Create a Glue database, table, and crawler for partitioned parquet data
    """
    # Get IAM role ARN from Terraform
    iam_role_arn = role
    if not iam_role_arn:
        raise ValueError("Could not get IAM role ARN from Terraform output")

    # Initialize Glue client
    glue_client = boto3.client("glue", region_name=region_name)

    try:
        # Step 1: Create Database
        print(f"Creating database: {database_name}")
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": "Database for IATA test data",
                }
            )
            print(f"Database {database_name} created successfully")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                print(f"Database {database_name} already exists")
            else:
                raise

        # Step 2: Create Table
        print(f"Creating table: {table_name}")
        try:
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    "Name": table_name,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "transaction_id", "Type": "string"},
                            {"Name": "amount", "Type": "double"},
                            {"Name": "date_time", "Type": "timestamp"},
                        ],
                        "Location": s3_path,
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"},
                        },
                    },
                    "PartitionKeys": [{"Name": "Country", "Type": "string"}],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "classification": "parquet",
                        "has_encrypted_data": "false",
                        "parquet.compression": "SNAPPY",
                    },
                },
            )
            print(f"Table {table_name} created successfully")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                print(f"Table {table_name} already exists")
            else:
                raise

        # Step 3: Create Crawler
        crawler_name = f"{database_name}_{table_name}_crawler"
        print(f"Creating crawler: {crawler_name}")

        try:
            glue_client.create_crawler(
                Name=crawler_name,
                Role=iam_role_arn,
                DatabaseName=database_name,
                Targets={"S3Targets": [{"Path": s3_path}]},
                TablePrefix="",
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
            )
            print(f"Crawler {crawler_name} created successfully")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                print(f"Crawler {crawler_name} already exists")
            else:
                raise

        # Step 4: Start Crawler
        print(f"Starting crawler: {crawler_name}")
        glue_client.start_crawler(Name=crawler_name)

        print("\nSetup completed successfully!")
        print("Note: Crawler is running and may take several minutes to complete.")
        print("You can check the crawler status in the AWS Glue Console or run:")
        print(f"aws glue get-crawler --name {crawler_name}")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create Athena Db")
    parser.add_argument("--role", required=True, help="Name of the S3 bucket")
    args = parser.parse_args()

    DATABASE_NAME = "iata_test_db"
    TABLE_NAME = "trd_table"
    S3_PATH = "s3://iata-test-data/trd/"
    REGION = "us-east-1"

    create_glue_database_and_table(
        database_name=DATABASE_NAME,
        table_name=TABLE_NAME,
        s3_path=S3_PATH,
        role=args.role,
        region_name=REGION,
    )
