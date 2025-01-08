resource "aws_cloudwatch_log_group" "sagemaker_pipeline_logs" {
  name = "/aws/sagemaker/ProcessingJobs"
  retention_in_days = 14
}


resource "aws_iam_policy" "sagemaker_logging_policy" {
  name        = "SageMakerLoggingPolicy"
  description = "Policy to allow SageMaker to write logs to CloudWatch"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_logging_policy_attachment" {
  policy_arn = aws_iam_policy.sagemaker_logging_policy.arn
  role       = aws_iam_role.sagemaker_execution_role.name
}