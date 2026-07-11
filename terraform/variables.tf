variable "aws_region" {
  description = "AWS Region used for the experiment."
  type        = string
  default     = "ap-northeast-1"
}

variable "project_name" {
  description = "Prefix used for AWS resource names. Keep it short enough for Lambda's 64-character limit."
  type        = string
  default     = "sqs-fair-queue-lt"

  validation {
    condition     = length(var.project_name) <= 32
    error_message = "project_name must be 32 characters or fewer."
  }
}

variable "lambda_zip_path" {
  description = "Path to the ARM64 Go Lambda deployment zip, relative to the terraform directory or absolute."
  type        = string
  default     = "../build/lambda/consumer.zip"
}

variable "lambda_memory_size" {
  description = "Consumer Lambda memory in MiB."
  type        = number
  default     = 256
}

variable "lambda_timeout_seconds" {
  description = "Consumer Lambda timeout. SQS visibility timeout is kept at six times this value."
  type        = number
  default     = 30
}

variable "reserve_concurrency" {
  description = "Reserve maximum concurrency plus five for each function. Size the account quota using reserved_concurrency_total plus Lambda's required unreserved concurrency pool."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention period."
  type        = number
  default     = 7
}
