data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "minnow" {
  name_prefix = "${var.name}-"
  description = "HTTPS access to Minnow; SSH is intentionally disabled"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "HTTP for Caddy ACME redirect/challenge"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Minnow HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.allowed_ipv4_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role" "minnow" {
  name_prefix = "${var.name}-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.minnow.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "secret_parameters" {
  name_prefix = "${var.name}-secrets-"
  role        = aws_iam_role.minnow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["ssm:GetParameter", "ssm:GetParameters"]
      Resource = [
        "arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${var.openai_api_key_parameter_name}",
        "arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${var.minnow_token_parameter_name}",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "minnow" {
  name_prefix = "${var.name}-"
  role        = aws_iam_role.minnow.name
}

resource "aws_instance" "minnow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids = [aws_security_group.minnow.id]
  iam_instance_profile   = aws_iam_instance_profile.minnow.name

  depends_on = [
    aws_iam_role_policy_attachment.ssm,
    aws_iam_role_policy.secret_parameters,
  ]

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  dynamic "credit_specification" {
    for_each = can(regex("^t(2|3|3a|4g)\\.", var.instance_type)) ? [1] : []
    content {
      cpu_credits = "standard"
    }
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.data_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  # Do not replace this stateful instance automatically when bootstrap changes;
  # apply runtime/image updates through SSM or deliberately replace after backup.
  user_data_replace_on_change = false
  user_data_base64 = base64encode(templatefile("${path.module}/user-data.sh.tftpl", {
    minnow_image          = var.minnow_image
    minnow_domain         = var.domain
    region                = var.region
    openai_parameter_name = var.openai_api_key_parameter_name
    token_parameter_name  = var.minnow_token_parameter_name
  }))

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [ami, user_data_base64]
  }

  tags = {
    Name = var.name
  }
}

resource "aws_eip" "minnow" {
  domain   = "vpc"
  instance = aws_instance.minnow.id
  tags = {
    Name = var.name
  }
}
