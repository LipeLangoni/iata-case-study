
provider "aws" {
  region = "us-east-1" 
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = "iata-test-data" 

  tags = {
    Name        = "SageMaker Data Bucket"
    Environment = "Development"
  }
}

resource "aws_s3_bucket_policy" "data_policy" {
  bucket = aws_s3_bucket.data_bucket.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "sagemaker.amazonaws.com"
        },
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "${aws_s3_bucket.data_bucket.arn}",
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

output "bucket_name" {
  value = aws_s3_bucket.data_bucket.id
}
