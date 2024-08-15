
variable "log_group_name" {
  description = "The name of the CloudWatch log group"
  type        = string
  default     = "vinson-extract-log-group"
}


variable "extract_lambda_func" {
  type = string
  default = "extract_handler"

}