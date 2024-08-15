

resource "aws_cloudwatch_log_group" "extract_log_group" {
  name              = var.log_group_name
  retention_in_days = 14
}


resource "aws_cloudwatch_log_stream" "extract_log_stream" {
  name           = "vinson-extract-log-group"
  log_group_name = aws_cloudwatch_log_group.extract_log_group.name
}



resource "aws_cloudwatch_log_metric_filter" "lambda-extract-error-catcher" {
  name           = "lamdba-extract-error-catcher"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.extract_log_group.name
  metric_transformation {
    name      = "ExtractError"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}


resource "aws_cloudwatch_metric_alarm" "cloudwatch_error_alarm" {
  alarm_name          = "HighErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.lambda-extract-error-catcher.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.lambda-extract-error-catcher.metric_transformation[0].namespace
  period              = 300 
  statistic           = "Sum"
  threshold           = 1 
  alarm_description   = "Alarm when there are more than 1 errors in the data pipeline within 5 minutes"
  alarm_actions       = [aws_sns_topic.topic.arn]
  ok_actions          = [aws_sns_topic.topic.arn]
  insufficient_data_actions = []
}


resource "aws_sns_topic" "topic" {
  name = "lamdba-extract-error-catcher"
}


resource "aws_sns_topic_subscription" "email-target" {
  for_each = toset(["andreabiro90@gmail.com", "edith.cheler5@gmail.com", "svelana.a.wise@gmail.com", "elymanwork@gmail.com", "danielsmyth@me.com", "mohsin.312007@gmail.com"])
  topic_arn = aws_sns_topic.topic.arn
  protocol  = "email"
  endpoint  = each.value
}