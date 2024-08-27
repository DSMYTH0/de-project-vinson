
          #########################################
#     ~~~~#### Extract Lambda Cloudwatch stuff ####~~~~
          #########################################


resource "aws_cloudwatch_log_group" "extract_log_group" {
  name              = var.extract_log_group_name
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "extract_log_stream" {
  name           = "vinson-extract-log-stream"
  log_group_name = aws_cloudwatch_log_group.extract_log_group.name
}

resource "aws_cloudwatch_log_metric_filter" "lambda_extract_error_catcher" {
  name           = "lambda-extract-error-catcher"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.extract_log_group.name

  metric_transformation {
    name      = "ExtractError"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "cloudwatch_error_alarm_1" {
  alarm_name          = "HighErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.lambda_extract_error_catcher.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.lambda_extract_error_catcher.metric_transformation[0].namespace
  period              = 300 
  statistic           = "Sum"
  threshold           = 1 
  alarm_description   = "Alarm when there are more than 1 errors in the data pipeline within 5 minutes"
  alarm_actions       = [aws_sns_topic.extract_topic.arn]
  ok_actions          = [aws_sns_topic.extract_topic.arn]
  insufficient_data_actions = [aws_sns_topic.extract_topic.arn]
}

resource "aws_sns_topic" "extract_topic" {
  name = "lambda-extract-error-catcher"
}

resource "aws_sns_topic_subscription" "extract_email_target" {
  for_each = toset(["projectvinson.team@gmail.com"])
  topic_arn = aws_sns_topic.extract_topic.arn
  protocol  = "email"
  endpoint  = each.value
}


          #########################################
#     ~~~~#### Transform Lambda Cloudwatch stuff ####~~~~
          #########################################



resource "aws_cloudwatch_log_group" "transform_log_group" {
  name              = var.transform_log_group_name
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "transform_log_stream" {
  name           = "vinson-transform-log-stream"
  log_group_name = aws_cloudwatch_log_group.transform_log_group.name
}

resource "aws_cloudwatch_log_metric_filter" "lambda_transform_error_catcher" {
  name           = "lambda-transform-error-catcher"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.transform_log_group.name

  metric_transformation {
    name      = "TransformError"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "cloudwatch_error_alarm_2" {
  alarm_name          = "HighErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.lambda_transform_error_catcher.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.lambda_transform_error_catcher.metric_transformation[0].namespace
  period              = 300 
  statistic           = "Sum"
  threshold           = 1 
  alarm_description   = "Alarm when there are more than 1 errors in the data pipeline within 5 minutes"
  alarm_actions       = [aws_sns_topic.transform_topic.arn]
  ok_actions          = [aws_sns_topic.transform_topic.arn]
  insufficient_data_actions = [aws_sns_topic.transform_topic.arn]
}

resource "aws_sns_topic" "transform_topic" {
  name = "lambda-transform-error-catcher"
}

resource "aws_sns_topic_subscription" "transform_email_target" {
  for_each = toset(["projectvinson.team@gmail.com"])
  topic_arn = aws_sns_topic.transform_topic.arn
  protocol  = "email"
  endpoint  = each.value
}




          #########################################
#     ~~~~#### Load Lambda Cloudwatch Resources ####~~~~
          #########################################



resource "aws_cloudwatch_log_group" "load_log_group" {
  name              = var.load_log_group_name
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "load_log_stream" {
  name           = "vinson-load-log-stream"
  log_group_name = aws_cloudwatch_log_group.load_log_group.name
}

resource "aws_cloudwatch_log_metric_filter" "lambda_loading_error_catcher" {
  name           = "lamdba-loading-error-catcher"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.load_log_group.name

  metric_transformation {
    name      = "LoadError"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "cloudwatch_error_alarm_3" {
  alarm_name          = "HighErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.lambda_loading_error_catcher.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.lambda_loading_error_catcher.metric_transformation[0].namespace
  period              = 300 
  statistic           = "Sum"
  threshold           = 1 
  alarm_description   = "Alarm when there are more than 1 errors in the data pipeline within 5 minutes"
  alarm_actions       = [aws_sns_topic.loading_topic.arn]
  ok_actions          = [aws_sns_topic.loading_topic.arn]
  insufficient_data_actions = [aws_sns_topic.loading_topic.arn]
}


resource "aws_sns_topic" "loading_topic" {
  name = "lamdba-loading-error-catcher"
}

resource "aws_sns_topic_subscription" "load_email_target" {
  for_each = toset(["mohsin.312007@gmail.com"])
  topic_arn = aws_sns_topic.loading_topic.arn
  protocol  = "email"
  endpoint  = each.value
}