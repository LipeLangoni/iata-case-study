resource "aws_iam_role" "glue_role" {
  name = "AWSGlueServiceRole-IATA"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "glue-service-role"
    Environment = "test"
  }
}

# Attach AWS Managed Policy for Glue Service
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Create Custom Policy for S3 Access
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "GlueS3Access-IATA"
  description = "Policy for Glue to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::iata-test-data",
          "arn:aws:s3:::iata-test-data/*"
        ]
      }
    ]
  })
}

# Attach S3 Policy to Glue Role
resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# Output the Role ARN (you'll need this for the Python script)
output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}