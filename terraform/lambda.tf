 
          #######################################
#     ~~~~##### Resources for Extract Lambda ####~~~~
          #######################################


# Creates a zip file from the source code for the lambda to use
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract/function.zip"
  source_file = "${path.module}/../src/extract/extract.py"        # When transform lambda is deployed, this line needs changing to source_dir
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
    role = aws_iam_role.transform_lambda_role.arn
    handler = "transform.${var.transform_lambda_func}"
    runtime = "python3.12"
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:12"] # Layers may need changing or adding, depends on AB & EC's needs
    timeout = 120
    memory_size = 512

    environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
    }
  }
}


          ########################################
#     ~~~~#### Resources for Load Lambda ####~~~~
          ########################################


# Creates a zip file from the source code for the Lambda to use
data "archive_file" "load_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/load/function.zip"
  source_file = "${path.module}/../src/load/load.py"
}

# Points to where the zipped code is located
resource "aws_s3_object" "load_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "packages/load/function.zip"
  source = "${path.module}/../packages/load/function.zip"
}

# Define the Load Lambda function
resource "aws_lambda_function" "load_lambda" {
    function_name = var.load_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/load/function.zip"
    role = aws_iam_role.load_lambda_role.arn
    handler = "load.${var.load_lambda_func}"
    runtime = "python3.12"
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:12"] # Layers may need changing or adding, depends on AB & EC's needs
    timeout = 180
    memory_size = 512
    source_code_hash = data.archive_file.load_lambda_zip.output_base64sha256

    environment {
      variables = {
        S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
      }
  }
}

# #Updates lambda code
# resource "terraform_data" "bootstrap" {
#   triggers_replace = [aws_lambda_function.load_lambda.id]
#   }
 