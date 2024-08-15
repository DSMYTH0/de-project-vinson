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

resource "aws_iam_policy" "s3_put_object_policy" {
  name   = "s3-policy-extract-lambda"
  policy = data.aws_iam_policy_document.s3_put_object_document.json
}

resource "aws_iam_role_policy_attachment" "extract_lambda_s3_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.s3_put_object_policy.arn
}







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

resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-currency-lambda-"
  policy      = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}