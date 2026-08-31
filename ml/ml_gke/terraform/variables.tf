variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The region to deploy resources"
  type        = string
  default     = "asia-southeast2"
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
  type        = string
  default     = "credit_bureau"
}
