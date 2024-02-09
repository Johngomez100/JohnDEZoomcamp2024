variable "credentials" {
  description = "My credentials"
  default     = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "metal-incline-412021"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My Big Query Dataset Name"
  default     = "demo_dataset"
}


variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "metal-incline-412021-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage class"
  default     = "STANDARD"
}
