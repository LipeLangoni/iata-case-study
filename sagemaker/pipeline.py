import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ScriptProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
import subprocess


def get_terraform_output(output_name):
    result = subprocess.run(["terraform", "output", "-raw", output_name], stdout=subprocess.PIPE)
    return result.stdout.decode("utf-8").strip()


sagemaker_session = sagemaker.Session()
role = get_terraform_output("sagemaker_execution_role_arn")
image_uri = sagemaker.image_uris.retrieve("xgboost", sagemaker_session.boto_region_name)

http_url = ParameterString(name="HttpUrl", default_value="https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip")
s3_bucket = ParameterString(name="S3Bucket", default_value=get_terraform_output("bucket_name"))
s3_prefix_raw = ParameterString(name="S3PrefixRaw", default_value="lnd/")
s3_prefix_unzipped = ParameterString(name="S3PrefixUnzipped", default_value="raw/")
s3_prefix_converted = ParameterString(name="S3PrefixConverted", default_value="trusted/")

fetch_data_processor = ScriptProcessor(
    image_uri=image_uri,
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    command=["python3"]
)

unzip_data_processor = ScriptProcessor(
    image_uri=image_uri,
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    command=["python3"]
)

spark_processor = PySparkProcessor(
    base_job_name="spark-preprocessor",
    framework_version="2.4",
    role=role,
    instance_count=2,
    instance_type="ml.m5.xlarge",
    max_runtime_in_seconds=1200,
)

spark_step = ProcessingStep(
    name="SparkProcessingStep",
    processor=spark_processor,
    inputs=[
        ProcessingInput(source=f"s3://{s3_bucket}/{s3_prefix_unzipped}", destination="/opt/ml/processing/input")
    ],
    outputs=[
        ProcessingOutput(source="/opt/ml/processing/output", destination=f"s3://{s3_bucket}/{s3_prefix_converted}")
    ],
    code="convert.py",  
    job_arguments=[
        's3_input_bucket', s3_bucket,
        's3_input_key_prefix', s3_prefix_unzipped,
        's3_output_bucket', s3_bucket,
        's3_output_key_prefix', s3_prefix_converted
    ]
)

fetch_data_step = ProcessingStep(
    name="FetchDataStep",
    processor=fetch_data_processor,
    inputs=[],
    outputs=[],
    job_arguments=[
        "--http_url", http_url,
        "--s3_bucket", s3_bucket,
        "--s3_prefix", s3_prefix_raw
    ],
    code="fetch.py"
)

unzip_data_step = ProcessingStep(
    name="UnzipDataStep",
    processor=unzip_data_processor,
    inputs=[],
    outputs=[],
    job_arguments=[
        "--s3_bucket", s3_bucket,
        "--input_prefix", s3_prefix_raw,
        "--output_prefix", s3_prefix_unzipped
    ],
    code="unzip.py"
)



pipeline = Pipeline(
    name="DataProcessingPipeline",
    parameters=[http_url, s3_bucket, s3_prefix_raw, s3_prefix_unzipped, s3_prefix_converted],
    steps=[fetch_data_step, unzip_data_step, spark_step],
    sagemaker_session=sagemaker_session
)

pipeline_definition = pipeline.definition()
pipeline.upsert(role_arn=role)


pipeline.start()