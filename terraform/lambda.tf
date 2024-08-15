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



resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../requirements.txt -t ${path.module}/../dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../requirements.txt")
  }
}

data "archive_file" "extract_lambda_dependencies_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/dependencies.zip"
  source_dir = "${path.module}/../dependencies"
}

resource "aws_s3_object" "lambda_requirements_layer_s3" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "packages/extract/dependencies.zip"
  source = "${path.module}/../packages/layers/dependencies.zip"
  etag = filemd5(data.archive_file.extract_lambda_dependencies_zip.output_path)
}


resource "aws_lambda_layer_version" "lambda_dependencies_layer" {
  layer_name = "lambda_dependencies_layer"
  s3_bucket = aws_s3_object.lambda_requirements_layer_s3.bucket
  s3_key = aws_s3_object.lambda_requirements_layer_s3.key
}


resource "aws_lambda_function" "extract_lambda" {
    function_name = var.extract_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/extract/function.zip"
    role = aws_iam_role.extract_lambda_role.arn
    handler = "extract.${var.extract_lambda_func}"
    runtime = "python3.12"
    layers = [aws_lambda_layer_version.lambda_dependencies_layer.arn]

    environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.code_bucket.bucket
    }
  }
}

