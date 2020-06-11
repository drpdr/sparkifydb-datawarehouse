output "local_ip" {
  #   value = <<-EOF
  #   Your local ip is: ${jsondecode(data.http.ifconfig.body).ip}
  #   EOF
  value = <<-EOF
  Your local ip is: ${jsondecode(data.http.ifconfig.body).ip_addr}
  EOF
}

# output "sparkify_role_output" {
#   value = aws_iam_role.sparkifydb_role
# }

# output "sparkify_cluster_endpoint" {
#   value = module.redshift.this_redshift_cluster_endpoint
# }
