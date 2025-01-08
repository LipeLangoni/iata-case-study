import pyarrow.dataset as ds

# Path to the parent folder containing all partitions
parent_folder_path = "s3://iata-test-data/trd/"

# Read the dataset with partitioning enabled
dataset = ds.dataset(parent_folder_path, format="parquet", partitioning="hive")

# Print the schema of the dataset
print(dataset.schema)
