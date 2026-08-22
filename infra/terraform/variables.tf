variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "ap-northeast-2"
}

variable "app_env" {
  description = "Application environment (prod, dev, local)"
  type        = string
  default     = "prod"
}

variable "raw_bucket" {
  description = "S3 bucket name for raw layer storage"
  type        = string
  default     = "ttareungyi-raw"
}

variable "seoul_api_key" {
  description = "Authentication key for Seoul Open Data Plaza API"
  type        = string
  sensitive   = true
}

variable "seoul_api_base_url" {
  description = "Base URL for Seoul Open Data Plaza API"
  type        = string
  default     = "http://openapi.seoul.go.kr:8088"
}

variable "slack_webhook_url" {
  description = "Slack incoming webhook URL for failure notifications"
  type        = string
  default     = ""
  sensitive   = true
}
