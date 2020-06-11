variable "region" {
  description = "AWS Region"
  # Udacity region
  default     = "us-west-2"
}


variable "role_name" {
  description = "A meaningful name"
  default     = "sparkifydb-role"
  type        = string
}

variable "cluster_identifier" {
  description = "Redshift cluster identifier"
  default     = "sparkify-redshift-cluster1"
}

variable "database_name" {
  description = "Name for the sparkify database"
  default     = "sparkifydb"
}


### WARNING: Sensitive data ###
variable "master_username" {
  description = "Redshift cluster master_username"
  default     = "sparkifyuser"
}

variable "master_password" {
  description = "Redshift cluster master_password"
}


