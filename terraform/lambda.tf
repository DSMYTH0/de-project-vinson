data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract/function.zip"
  source_file = "${path.module}/../src/extract/extract.py"
}


resource "aws_lambda_function" "extract_lambda" {
    function_name = var.extract_lambda_func
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "packages/extract/function.zip"
    role = aws_iam_role.lambda_role.arn
    handler = "extract.${var.extract_lambda_func}"
}

