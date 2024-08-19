 
          #######################################
#     ~~~~##### Resources for Extract Lambda ####~~~~
          #######################################


# Creates a zip file from the source code for the lambda to use
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract/function.zip"
  source_file = "${path.module}/../src/extract/extract.py"
}

# Points to where the zipped code is located
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "packages/extract/function.zip"
  source = "${path.module}/../packages/extract/function.zip"
}

# Define the extract Lambda function
resource "aws_lambda_function" "extract_lambda" {
    function_name = var.extract_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/extract/function.zip"
    role = aws_iam_role.extract_lambda_role.arn
    handler = "extract.${var.extract_lambda_func}"
    runtime = "python3.12"
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:12"]
    timeout = 30
    memory_size = 512

    environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
    }
  }
}

          ########################################
#     ~~~~#### Resources for Transform Lambda ####~~~~
          ########################################


# Creates a zip file from the source code for the Lambda to use
data "archive_file" "transform_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/transform/function.zip"
  source_file = "${path.module}/../src/transform/transform.py"
}

# Points to where the zipped code is located
resource "aws_s3_object" "transform_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "packages/transform/function.zip"
  source = "${path.module}/../packages/transform/function.zip"
}

# Define the Transform Lambda function
resource "aws_lambda_function" "transform_lambda" {
    function_name = var.transform_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/transform/function.zip"
    role = aws_iam_role.extract_lambda_role.arn
    handler = "extract.${var.extract_lambda_func}"
    runtime = "python3.12"
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:12"] # Layers may need changing or adding, depends on AB & EC's needs
    timeout = 30
    memory_size = 512

    environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
    }
  }
}