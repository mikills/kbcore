output "elastic_ip" {
  value = aws_eip.minnow.public_ip
}

output "minnow_url" {
  value = "https://${var.domain}"
}

output "ssm_start_session" {
  value = "aws ssm start-session --target ${aws_instance.minnow.id} --region ${var.region}"
}
