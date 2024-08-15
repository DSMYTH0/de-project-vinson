data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract/function.zip"
  source_file = "${path.module}/../src/extract/extract.py"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "packages/extract/function.zip"
  source = "${path.module}/../packages/extract/function.zip"
}

resource "aws_lambda_function" "extract_lambda" {
    function_name = var.extract_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/extract/function.zip"
    role = aws_iam_role.extract_lambda_role.arn
    handler = "extract.${var.extract_lambda_func}"
    runtime = "python3.12"

    environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
    }
  }
}

