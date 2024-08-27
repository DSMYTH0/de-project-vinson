          ######################################
#     ~~~~#### Extract Lambda IAM Resources ####~~~~
          ######################################


# Creates IAM role for extract lambda
resource "aws_iam_role" "extract_lambda_role" {
  name               = "extract_lambda_assume_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#    ~~~~#### S3 ####~~~~

# Creates policy to be attached to IAM role
resource "aws_iam_policy" "s3_put_object_policy" {
  name   = "s3-policy-extract-lambda"
  policy = data.aws_iam_policy_document.s3_put_object_document.json
}

# Creates policy document to attach to policy
data "aws_iam_policy_document" "s3_put_object_document" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "arn:aws:s3:::vinson-ingestion-zone/*"
    ]
  }
  statement {
    
    actions = ["s3:ListAllMyBuckets"]

    resources = ["arn:aws:s3:::*"]
  }
}

# Attaches the policy to the IAM role
resource "aws_iam_role_policy_attachment" "extract_lambda_s3_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.s3_put_object_policy.arn
}

#    ~~~~#### Cloudwatch ####~~~~

# Creates cloudwatch policy to attach to IAM role
resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-currency-lambda-"
  policy      = data.aws_iam_policy_document.cw_document.json
}

# Creates policy document to be attached to the cloudwatch policy
data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:*"
    ]
  }
}

# Attaches cloudwatch policy to the IAM role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}


#    ~~~~#### Secrets Manager ####~~~~


# Creates a secretsmanager policy to attach to the IAM role
resource "aws_iam_policy" "secrets_manager_policy" {
  name   = "secrets-manager-policy-extract-lambda"
  policy = data.aws_iam_policy_document.secrets_manager_access.json
}

# Creates a policy document to attach to the policy
data "aws_iam_policy_document" "secrets_manager_access" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:pg_*"]
  }
}

# Attaches the policy to the IAM role
resource "aws_iam_role_policy_attachment" "extract_lambda_secrets_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
}


          ######################################
#     ~~~~#### Transform Lambda IAM Resources ####~~~~
          ######################################


# Creates IAM role for transform lambda
resource "aws_iam_role" "transform_lambda_role" {
  name               = "transform_lambda_assume_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#    ~~~~#### S3 ####~~~~

# Creates policy to be attached to IAM role
resource "aws_iam_policy" "s3_access_both_buckets" {
  name   = "s3-policy-transform-lambda"
  policy = data.aws_iam_policy_document.s3_access_both_buckets_document.json
}

# Creates policy document to attach to policy
data "aws_iam_policy_document" "s3_access_both_buckets_document" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "arn:aws:s3:::vinson-ingestion-zone/*",
      "arn:aws:s3:::vinson-processed-zone/*"
    ]
  }
  statement {
    
    actions = ["s3:ListAllMyBuckets"]

    resources = ["arn:aws:s3:::*"]
  }
}

# Attaches the policy to the IAM role
resource "aws_iam_role_policy_attachment" "transform_lambda_s3_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.s3_access_both_buckets.arn
}

# Allows Transform Lambda to be invoked by S3
resource "aws_lambda_permission" "allow_s3_invoke_lambda" {
  statement_id  = "AllowS3InvokeLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::vinson-ingestion-zone"
}

# S3 Bucket Notification to trigger Lambda
resource "aws_s3_bucket_notification" "ingestion_zone_notification" {
  bucket = "vinson-ingestion-zone"

  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_lambda]
}

#    ~~~~#### Cloudwatch ####~~~~


# Attaches cloudwatch policy to the IAM role
resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}




          ######################################
#     ~~~~#### Load Lambda IAM Resources ####~~~~
          ######################################


# Creates IAM role for transform lambda
resource "aws_iam_role" "load_lambda_role" {
  name               = "load_lambda_assume_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#    ~~~~#### S3 Policy ####~~~~

# Creates policy document to attach to policy
data "aws_iam_policy_document" "s3_get_object_document" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "arn:aws:s3:::vinson-processed-zone/*"
    ]
  }
  statement {
    
    actions = ["s3:ListAllMyBuckets"]

    resources = ["arn:aws:s3:::*"]
  }
}

# Creates policy to be attached to IAM role
resource "aws_iam_policy" "s3_get_policy" {
  name   = "s3-policy-load-lambda"
  policy = data.aws_iam_policy_document.s3_get_object_document.json
}

# Attaches the policy to the IAM role
resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_get_policy.arn
}


#    ~~~~#### S3 Permission ####~~~~

# Allows Load Lambda to be invoked by S3
resource "aws_lambda_permission" "allow_s3_invoke_load_lambda" {
  statement_id  = "AllowS3InvokeLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::vinson-processed-zone"
}

# S3 Bucket Notification to trigger Lambda
resource "aws_s3_bucket_notification" "processed_zone_notification" {
  bucket = "vinson-processed-zone"

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_load_lambda]
}



#    ~~~~#### Cloud Watch Attachement ####~~~~

# Attaches cloudwatch policy to the IAM role
resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}
