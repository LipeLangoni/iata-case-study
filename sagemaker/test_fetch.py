import subprocess
import os
import zipfile
import boto3

def get_terraform_output(output_name):
    result = subprocess.run(["terraform", "output", "-raw", output_name], stdout=subprocess.PIPE,check=True,cwd="../terraform")
    return result.stdout.decode("utf-8").strip()

def test_script_execution():
    http_url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'
    s3_bucket = get_terraform_output("bucket_name")
    s3_prefix = 'lnd/'
    
    result = subprocess.run(
        ['python3', 'fetch.py', '--http_url', http_url, '--s3_bucket', s3_bucket, '--s3_prefix', s3_prefix],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    
    assert result.returncode == 0, f"Script failed with return code {result.returncode}"
    
    zip_file_path = '/tmp/output_data.zip'
    assert os.path.exists(zip_file_path), "Zip file was not created"
    
    assert zipfile.is_zipfile(zip_file_path), "Output file is not a valid zip file"
    
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    
    files_in_s3 = [obj['Key'] for obj in response.get('Contents', [])]
    expected_file_key = os.path.join(s3_prefix, os.path.basename(zip_file_path))
    
    assert expected_file_key in files_in_s3, f"File {expected_file_key} was not found in S3 bucket {s3_bucket}"

if __name__ == "__main__":
    test_script_execution()