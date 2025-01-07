resource "aws_iam_role" "sagemaker_execution_role" {
  name               = "sagemaker_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = [
            "sagemaker.amazonaws.com",
            "states.amazonaws.com"
          ]
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "sagemaker_policy" {
  name = "SageMakerPolicy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Existing permissions
      {
        Effect = "Allow",
        Action = [
          "s3:*",
          "glue:*",
          "athena:*",
          "sagemaker:AddTags",
          "sagemaker:CreateProcessingJob"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = "iam:PassRole",
        Resource = [
          "arn:aws:iam::995227787260:role/sagemaker_execution_role",
          "arn:aws:iam::995227787260:role/sagemaker-pipeline-*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ],
        Resource = "*"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "sagemaker_role_policy_attachment" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_policy.arn
}

output "sagemaker_execution_role_arn" {
  value = aws_iam_role.sagemaker_execution_role.arn
}
