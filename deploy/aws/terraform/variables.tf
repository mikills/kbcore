variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type    = string
  default = "minnow"
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "minnow_image" {
  type        = string
  description = "Public or authenticated registry image, pinned by tag or digest"
}

variable "domain" {
  type        = string
  description = "DNS name that resolves to the instance Elastic IP"
}

variable "openai_api_key_parameter_name" {
  type        = string
  description = "Name of an existing SSM SecureString containing the OpenAI API key"
  default     = "/minnow/openai-api-key"

  validation {
    condition     = can(regex("^/[A-Za-z0-9_.\\-/]+$", var.openai_api_key_parameter_name))
    error_message = "openai_api_key_parameter_name must be an absolute SSM parameter path."
  }
}

variable "minnow_token_parameter_name" {
  type        = string
  description = "Name of an existing SSM SecureString containing exactly 64 hexadecimal characters"
  default     = "/minnow/bearer-token"

  validation {
    condition     = can(regex("^/[A-Za-z0-9_.\\-/]+$", var.minnow_token_parameter_name))
    error_message = "minnow_token_parameter_name must be an absolute SSM parameter path."
  }
}

variable "allowed_ipv4_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach HTTPS; use [\"0.0.0.0/0\"] for public clients"
  default     = ["0.0.0.0/0"]
}

variable "data_volume_gb" {
  type    = number
  default = 30
}
