import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ScriptProcessor
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
import subprocess


def get_terraform_output(output_name):
    result = subprocess.run(["terraform", "output", "-raw", output_name], stdout=subprocess.PIPE, cwd="../terraform")
    return result.stdout.decode("utf-8").strip()


sagemaker_session = sagemaker.Session()
role = get_terraform_output("sagemaker_execution_role_arn")
image_uri = sagemaker.image_uris.retrieve(
    framework="linear-learner",
    region=sagemaker.Session().boto_region_name
)

http_url = ParameterString(name="HttpUrl", default_value="https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip")
s3_bucket = ParameterString(name="S3Bucket", default_value=get_terraform_output("bucket_name"))
s3_prefix_raw = ParameterString(name="S3PrefixRaw", default_value="lnd/")
s3_prefix_unzipped = ParameterString(name="S3PrefixUnzipped", default_value="raw/")
s3_prefix_converted = ParameterString(name="S3PrefixConverted", default_value="trd/")

script_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_count=1,
    instance_type="ml.t3.medium",
    command=["python3"]
)

spark_processor = PySparkProcessor(
    base_job_name="spark-preprocessor",
    framework_version="2.4",
    role=role,
    instance_count=2,
    instance_type="ml.t3.medium",
    max_runtime_in_seconds=1200,
)

fetch_data_step = ProcessingStep(
    name="FetchDataStep",
    processor=script_processor,
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
    processor=script_processor,
    inputs=[],
    outputs=[],
    job_arguments=[
        "--s3_bucket", s3_bucket,
        "--input_key", 'lnd/output_data.zip',
        "--output_prefix", s3_prefix_unzipped
    ],
    depends_on=[fetch_data_step.name],
    code="unzip.py"
)

spark_step = ProcessingStep(
    name="SparkProcessingStep",
    processor=spark_processor,
    code="convert.py",  
    job_arguments=[
        '--s3_bucket', s3_bucket,
        '--input_key', 'raw/2m Sales Records.csv',
        '--output_prefix', s3_prefix_converted
    ],
    depends_on=[unzip_data_step.name]
)

athena_processing_step = ProcessingStep(
    name="RunAthenaQuery",
    processor=script_processor,
    code='athena.py',
    depends_on=[spark_step.name]
)

pipeline = Pipeline(
    name="DataProcessingPipeline",
    parameters=[http_url, s3_bucket, s3_prefix_raw, s3_prefix_unzipped, s3_prefix_converted],
    steps=[fetch_data_step, unzip_data_step, spark_step, athena_processing_step],
    sagemaker_session=sagemaker_session
)

pipeline_definition = pipeline.definition()
pipeline.upsert(role_arn=role)


pipeline.start(execution_display_name="debug-execution")