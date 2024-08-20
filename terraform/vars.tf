
variable "extract_log_group_name"{
  description = "The name of the CloudWatch log group for the extract lambda"
  type        = string
  default     = "/aws/lambda/extract_handler"
}


variable "extract_lambda_func" {
  type = string
  default = "extract_handler"

}


variable "transform_log_group_name"{
  description = "The name of the Cloudwatch log group for the transform lambda"
  type        = string
  default     = "aws/lambda/transform_handler"
}

variable "transform_lambda_func" {
  type = string
  default = "transform_handler"

}