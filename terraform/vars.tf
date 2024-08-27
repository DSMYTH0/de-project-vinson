
variable "extract_log_group_name"{
  description = "The name of the CloudWatch log group for the extract lambda"
  type        = string
  default     = "vinson_extract_handler"
}


variable "extract_lambda_func" {
  type = string
  default = "extract_handler"

}


variable "transform_log_group_name"{
  description = "The name of the Cloudwatch log group for the transform lambda"
  type        = string
  default     = "vinson_transform_handler"
}

variable "transform_lambda_func" {
  type = string
  default = "transform_handler"

}


#Create variables for Load Lambda
variable "load_log_group_name"{
  description = "The name of the Cloudwatch log group for the load lambda"
  type        = string
  default     = "vinson_load_handler"
}

variable "load_lambda_func" {
  type = string
  default = "load_handler"
}