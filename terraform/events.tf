resource "aws_cloudwatch_event_rule" "every_fifteen_minutes" {
    name = "every-fifteen-minutes"
    description = "Fires every fifteen minutes"
    schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "check_extract_lambda_every_fifteen_minutes" {
    rule = aws_cloudwatch_event_rule.every_fifteen_minutes.name
    target_id = "extract_lambda"
    arn = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_check_extract_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_fifteen_minutes.arn
}