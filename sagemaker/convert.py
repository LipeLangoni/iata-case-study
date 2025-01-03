from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3 File Conversion and Partitioning") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

input_path = "s3://your-bucket-name/path-to-unzipped-file/"
output_path = "s3://your-bucket-name/output-folder/"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(input_path)

df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("Country") \
    .save(output_path)

spark.stop()
