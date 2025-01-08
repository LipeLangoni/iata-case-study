import boto3
import time

# Athena and S3 configurations
athena_client = boto3.client("athena")
s3_bucket = "iata-test-data"
s3_prefix_converted = "trd/"
database_name = "iata_athena_db"
table_name = "sales_data"


def execute_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": f"s3://{s3_bucket}/athena-results/"},
    )
    query_execution_id = response["QueryExecutionId"]

    while True:
        status_response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = status_response["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED"]:
            break
        time.sleep(2)

    if status == "FAILED":
        error_message = status_response["QueryExecution"]["Status"]["StateChangeReason"]
        raise Exception(
            f"Athena query failed with status: {status}. Reason: {error_message}"
        )

    print("Query succeeded!")


def create_athena_database():
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
    execute_athena_query(create_db_query)


def create_athena_table():
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS sales_data (
      region STRING,
      item_type STRING,
      sales_channel STRING,
      order_priority STRING,
      order_date STRING,
      order_id BIGINT,
      ship_date STRING,
      units_sold INT,
      unit_price FLOAT,
      unit_cost FLOAT,
      total_revenue FLOAT,
      total_cost FLOAT,
      total_profit FLOAT
    )
    PARTITIONED BY (country STRING)
    STORED AS PARQUET
    LOCATION 's3://{s3_bucket}/{s3_prefix_converted}';
    """
    execute_athena_query(create_table_query)


def add_partitions():
    add_partition_query = """
    MSCK REPAIR TABLE sales_data;
    """
    execute_athena_query(add_partition_query)


if __name__ == "__main__":
    print("Creating Athena database...")
    create_athena_database()
    print("Creating Athena table...")
    create_athena_table()
    print("Adding partitions...")
    add_partitions()
