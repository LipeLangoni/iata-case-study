import boto3

sagemaker_client = boto3.client('sagemaker')

pipeline_name = "DataProcessingPipeline"
response = sagemaker_client.list_pipeline_executions(
    PipelineName=pipeline_name,
    MaxResults=1
)

execution_arn = response['PipelineExecutionSummaries'][0]['PipelineExecutionArn']
response = sagemaker_client.list_pipeline_execution_steps(
    PipelineExecutionArn=execution_arn
)

fetch_data_step = next(
    (step for step in response['PipelineExecutionSteps'] if step['StepName'] == 'FetchDataStep'), 
    None
)

if fetch_data_step:
    processing_job_arn = fetch_data_step['Metadata']['ProcessingJob']['Arn']
    print(f"Fetching details for Processing Job: {processing_job_arn}")

    # Get the processing job details
    processing_job_details = sagemaker_client.describe_processing_job(
        ProcessingJobName=processing_job_arn.split('/')[-1]
    )

    # Print relevant information
    print("Status:", processing_job_details['ProcessingJobStatus'])
    if 'FailureReason' in processing_job_details:
        print("Failure Reason:", processing_job_details['FailureReason'])
    print("Processing Job Logs URL:", processing_job_details['ProcessingJobAppSpecification']['ContainerEntrypoint'])
else:
    print("FetchDataStep not found.")