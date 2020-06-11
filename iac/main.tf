# Terraform constraints
terraform {
  required_version = ">= 0.12"
}

# Locals
# If needed

# AWS provider attributes
# You need to have your credentials set through aws_cli or
# in environment variables
provider "aws" {
  version = "~> 2.0"
  region  = var.region
}

# Data Sources
data "aws_iam_policy" "s3_readonly" {
  arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

provider "http" {
  # avoid possible major versions breaking changes
  version = "~> 1.2"
}

# Get my local IP address
# https://www.terraform.io/docs/providers/http/data_source.html
data "http" "ifconfig" {
  # url = "https://ifconfig.co"
  url = "https://ifconfig.me/all.json"
  # Optional request headers
  request_headers = {
    Accept = "application/json"
  }
}

# 1. IAM Role (TODO: use iam_role_assumeRole readonly)?
resource "aws_iam_role" "sparkifydb_role" {
  name = var.role_name

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF

  tags = {
    use   = "udacity"
    topic = "sparkify"
  }
}

# Attach an S3 readonly policy to the role
resource "aws_iam_role_policy_attachment" "sparkify_role_policy" {
  depends_on = [aws_iam_role.sparkifydb_role]
  role       = aws_iam_role.sparkifydb_role.name
  policy_arn = data.aws_iam_policy.s3_readonly.arn
}


######
# VPC
# https://docs.aws.amazon.com/redshift/latest/mgmt/getting-started-cluster-in-vpc.html
######
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 2.0"

  name = "udacity-vpc"

  cidr = "10.10.0.0/16"
  # enable_nat_gateway = true

  azs = ["eu-south-1a", "eu-south-1b", "eu-south-1c"]
  # private_subnets = ["10.10.41.0/24"]
  #, "10.10.42.0/24", "10.10.43.0/24"]
  public_subnets = ["10.10.101.0/24", "10.10.102.0/24", "10.10.103.0/24", "10.10.104.0/24"]

  tags = {
    use   = "udacity"
    topic = "sparkify"
  }
}

resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name       = "redshift-subnet-group"
  subnet_ids = module.vpc.public_subnets

}



###########################
# VPC Security group specific for Redshift
# https://github.com/terraform-aws-modules/terraform-aws-security-group/blob/master/modules/README.md
###########################
module "sg" {
  source  = "terraform-aws-modules/security-group/aws//modules/redshift"
  version = "~> 3.0"

  name   = "sparkify-redshift-cluster-sg"
  vpc_id = module.vpc.vpc_id

  # Allow ingress rules to be accessed only by my ip address
  ingress_cidr_blocks = ["${jsondecode(data.http.ifconfig.body).ip_addr}/32"]

  # Allow all rules for all protocols
  egress_rules = ["all-all"]
}

# 2. Redshift cluster
resource "aws_redshift_cluster" "sparkify" {
  depends_on         = [aws_iam_role.sparkifydb_role]
  cluster_identifier = var.cluster_identifier
  database_name      = var.database_name
  master_username    = var.master_username
  master_password    = var.master_password
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes                     = 4
  automated_snapshot_retention_period = 0
  skip_final_snapshot                 = true
  iam_roles                           = [aws_iam_role.sparkifydb_role.arn]

  # default
  # publicly_accessible = true

  vpc_security_group_ids    = [module.sg.this_security_group_id]
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.id

  tags = {
    use   = "udacity"
    topic = "sparkify"
  }
}

# After creating the cluster, write a config file 
# with all the necessary information to connect
resource "local_file" "cfg-file" {
  depends_on = [aws_redshift_cluster.sparkify]
  filename   = "dwh.cfg"
  content    = <<-EOT
  [CLUSTER]
  HOST = ${element(split(":", aws_redshift_cluster.sparkify.endpoint), 0)}
  DB_NAME = ${aws_redshift_cluster.sparkify.database_name}
  DB_USER = ${var.master_username}
  DB_PASSWORD = ${var.master_password}
  DB_PORT = ${aws_redshift_cluster.sparkify.port}

  [IAM_ROLE]
  ARN = ${aws_iam_role.sparkifydb_role.arn}

  [S3]
  LOG_DATA = s3://udacity-dend/log_data
  LOG_JSONPATH = s3://udacity-dend/log_json_path.json
  SONG_DATA = s3://udacity-dend/song_data
  EOT
}