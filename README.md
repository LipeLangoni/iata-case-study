![Animation](assets/iata.drawio.png)

# IATA Case Study Project

This project automates the data processing workflow using Amazon SageMaker Pipelines and AWS services like S3, Glue, and Athena. The steps include fetching a zipped dataset from a URL, unzipping it, converting CSV to Parquet with partitioning, and setting up an Athena database for querying the processed data.


## Project Overview

The pipeline automates the following tasks:

1. **Download Data**: Fetches a ZIP file from a given URL and stores it in the "LND" zone of an S3 bucket.
2. **Unzip Data**: Unzips the data and saves it in the "RAW" zone of the same S3 bucket.
3. **Data Transformation**: Uses PySpark to convert CSV data to Parquet format, partitioning by the "Country" field, and saves the transformed data in the "TRD" zone of the S3 bucket.
4. **Create Athena Database**: Creates a Glue Athena database for querying the processed data.

## Prerequisites

- AWS Account with appropriate permissions to create and manage S3, SageMaker, Glue, and CloudWatch resources.
- Terraform for managing AWS infrastructure.
- Python and AWS SDK (Boto3) for interacting with AWS services.

## Architecture

- **S3 Buckets**: 
  - `lnd`: Zone for storing the raw data from the URL.
  - `raw`: Zone for unzipped data.
  - `trd`: Zone for storing transformed Parquet data partitioned by "Country".
  
- **SageMaker Pipeline**:
  - A series of steps that execute in sequence to fetch, process, and transform data.
  
- **Glue**:
  - A Glue database is created to enable Athena querying on the processed Parquet data.

## Steps

### 1. Fetch Data from URL
The pipeline first fetches data from the URL: https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip
It stores the ZIP file in the S3 bucket's `lnd` zone.

### 2. Unzip Data
The next step extracts the contents of the ZIP file and saves the data in CSV format to the `raw` zone in S3.

### 3. Convert CSV to Parquet with PySpark
The third step reads the unzipped CSV file, converts it to Parquet format, and partitions the data by the `Country` field. The output is saved in the `trd` zone in S3.

### 4. Create Glue Athena Database
The final step in the pipeline creates a Glue Athena database that allows querying the transformed data stored in the `trd` zone.

## Terraform Setup

Navigate to the `terraform` directory and run the following commands to set up the AWS infrastructure:

```
cd terraform
```

```
terraform init -backend-config="bucket=<your-s3-bucket-name>" -backend-config="key=terraform/state" -backend-config="region=<your-region>" && terraform apply
```

This will create:
- S3 buckets for different data zones (lnd, raw, trd)
- SageMaker role with necessary permissions
- CloudWatch log group for SageMaker Processing Job logs

---

## Running the SageMaker Pipeline

Navigate to the `sagemaker` directory and run the pipeline using the following command:

```
cd sagemaker
```

```
python3 pipeline.py
```

This script deploys the SageMaker Pipeline that:
1. Fetches the zipped dataset from the URL and stores it in the S3 `lnd` zone.
2. Unzips the dataset and saves the extracted files in the S3 `raw` zone.
3. Converts the extracted CSV to Parquet, partitions it by `Country`, and saves it in the S3 `trd` zone.
4. Creates an Athena database and table for querying the processed data.

Each step depends on the previous one, in the above order.

---

## Logs & Monitoring
All logs for the SageMaker processing jobs are stored in CloudWatch, which provides insights into job progress and any errors that might occur.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.