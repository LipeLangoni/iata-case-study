import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
import subprocess
from sagemaker.workflow.functions import Join


def get_terraform_output(output_name):
    result = subprocess.run(
        ["terraform", "output", "-raw", output_name],
        stdout=subprocess.PIPE,
        cwd="../terraform",
    )
    return result.stdout.decode("utf-8").strip()


sagemaker_session = sagemaker.Session()
role = get_terraform_output("sagemaker_execution_role_arn")
image_uri = sagemaker.image_uris.retrieve(
    framework="linear-learner", region=sagemaker.Session().boto_region_name
)

http_url = ParameterString(
    name="HttpUrl",
    default_value="https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip",
)
s3_bucket = ParameterString(
    name="S3Bucket", default_value=get_terraform_output("bucket_name")
)
s3_prefix_raw = ParameterString(name="S3PrefixRaw", default_value="lnd/")
s3_prefix_unzipped = ParameterString(name="S3PrefixUnzipped", default_value="raw/")
s3_prefix_converted = ParameterString(name="S3PrefixConverted", default_value="trd/")

fetch_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_count=1,
    instance_type="ml.t3.medium",
    command=["python3"],
)
unzip_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_count=1,
    instance_type="ml.t3.medium",
    command=["python3"],
)
athena_processor = SKLearnProcessor(
    framework_version="1.2-1",
    role=role,
    instance_count=1,
    instance_type="ml.t3.medium",
    command=["python3"],
)

spark_processor = PySparkProcessor(
    base_job_name="spark-preprocessor",
    framework_version="2.4",
    role=role,
    instance_count=1,
    instance_type="ml.t3.large",
    max_runtime_in_seconds=1200,
)

log_group = "/aws/sagemaker/pipelines"
log_stream = "pipeline-execution"

fetch_output = ProcessingOutput(
    output_name="fetch_output",
    source="/opt/ml/processing/output",
    destination=Join(on="", values=["s3://", s3_bucket, "/", s3_prefix_raw]),
)

fetch_data_step = ProcessingStep(
    name="FetchDataStep",
    processor=fetch_processor,
    outputs=[fetch_output],
    job_arguments=[
        "--http_url",
        http_url,
        "--s3_bucket",
        s3_bucket,
        "--s3_prefix",
        s3_prefix_raw,
    ],
    code="fetch.py",
)

unzip_input = ProcessingInput(
    source=fetch_output.destination,  # This ensures the input matches the previous output exactly
    destination="/opt/ml/processing/input",
)
unzip_output = ProcessingOutput(
    output_name="unzip_output",
    source="/opt/ml/processing/output",
    destination=Join(on="", values=["s3://", s3_bucket, "/", s3_prefix_unzipped]),
)

unzip_data_step = ProcessingStep(
    name="UnzipDataStep",
    processor=unzip_processor,
    inputs=[unzip_input],
    outputs=[unzip_output],
    job_arguments=[
        "--s3_bucket",
        s3_bucket,
        "--input_key",
        "lnd/output_data.zip",
        "--output_prefix",
        s3_prefix_unzipped,
    ],
    code="unzip.py",
    depends_on=[fetch_data_step.name],
)

spark_input = ProcessingInput(
    source=unzip_output.destination,  # This ensures the input matches the previous output exactly
    destination="/opt/ml/processing/input",
)
spark_output = ProcessingOutput(
    output_name="spark_output",
    source="/opt/ml/processing/output",
    destination=Join(on="", values=["s3://", s3_bucket, "/", s3_prefix_converted]),
)

spark_step = ProcessingStep(
    name="SparkProcessingStep",
    processor=spark_processor,
    inputs=[spark_input],
    outputs=[spark_output],
    code="convert.py",
    job_arguments=[
        "--s3_bucket",
        s3_bucket,
        "--input_key",
        "raw/2m Sales Records.csv",
        "--output_prefix",
        s3_prefix_converted,
    ],
    depends_on=[unzip_data_step.name],
)

athena_input = ProcessingInput(
    source=spark_output.destination,  # This ensures the input matches the previous output exactly
    destination="/opt/ml/processing/input",
)

athena_processing_step = ProcessingStep(
    name="RunAthenaQuery",
    processor=athena_processor,
    inputs=[athena_input],
    code="athena.py",
    depends_on=[spark_step.name],
    job_arguments=["--role", get_terraform_output("glue_role_arn")],
)

pipeline = Pipeline(
    name="DataProcessingPipeline",
    parameters=[
        http_url,
        s3_bucket,
        s3_prefix_raw,
        s3_prefix_unzipped,
        s3_prefix_converted,
    ],
    steps=[fetch_data_step, unzip_data_step, spark_step, athena_processing_step],
    sagemaker_session=sagemaker_session,
)

pipeline_definition = pipeline.definition()
pipeline.upsert(role_arn=role)


pipeline.start()
