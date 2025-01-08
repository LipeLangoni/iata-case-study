import subprocess
import os
import zipfile
import boto3

def get_terraform_output(output_name):
    result = subprocess.run(["terraform", "output", "-raw", output_name], stdout=subprocess.PIPE, check=True, cwd="../../terraform")
    return result.stdout.decode("utf-8").strip()

def test_script_execution():
    s3_bucket = get_terraform_output("bucket_name")
    input_key = 'raw/2m Sales Records.csv'
    output_prefix = 'trd/'
    result = subprocess.run(
        ['python3', '../convert.py', '--s3_bucket', s3_bucket, '--input_key', input_key, '--output_prefix', output_prefix],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    
    assert result.returncode == 0, f"Script failed with return code {result.returncode}"
    
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix="lnd/")
    
    files_in_s3 = [obj['Key'] for obj in response.get('Contents', [])]
    expected_file_key = os.path.join("lnd/", os.path.basename(input_key))
    
    assert expected_file_key in files_in_s3, f"File {expected_file_key} was not found in S3 bucket {s3_bucket}"
    
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=output_prefix)
    csv_files_in_raw = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    
    assert len(csv_files_in_raw) > 0, f"No CSV files found in the {output_prefix} folder in S3 bucket {s3_bucket}"

if __name__ == "__main__":
    test_script_execution()