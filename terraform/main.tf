provider "aws" {
  region = "us-east-1"
}

# TODO: Use AWS secret manager
variable "aws_access_key" {
  description = "AWS Access Key"
  type        = string
}
# TODO: Use AWS secret manager
variable "aws_secret_key" {
  description = "AWS Secret Key"
  type        = string
}

resource "aws_instance" "solana_indexer" {
  ami           = data.aws_ami.amazon_linux_2.id
  instance_type = "t2.micro"

  key_name = aws_key_pair.deployer.key_name

  vpc_security_group_ids = [aws_security_group.allow_ssh.id]

  tags = {
    Name = "SolanaIndexer"
  }

  user_data = <<-EOF
              #!/bin/bash
              set -e
              set -x

              exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

              # Log the public IP address
              echo "Public IP address: $(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"

              yum update -y
              amazon-linux-extras install docker -y
              service docker start
              usermod -a -G docker ec2-user
              chkconfig docker on

              # Install Docker Compose
              curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose

              # Clone your repository
              git clone https://github.com/lgingerich/solana-indexer /home/ec2-user/solana-indexer
              cd /home/ec2-user/solana-indexer

              # Log Docker version and info
              docker --version
              docker info

              # Build and run the Docker container
              docker build -t solana-indexer -f python/Dockerfile .
              docker run -d --name solana-indexer-container solana-indexer

              # Log the running containers
              docker ps -a
              EOF
}

resource "aws_key_pair" "deployer" {
  key_name   = "deployer-key"
  public_key = file("${path.module}/deployer-key.pub")
}

resource "aws_security_group" "allow_ssh" {
  name        = "allow_ssh"
  description = "Allow SSH inbound traffic"

  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "allow_ssh"
  }
}

# Use the AWS AMI data source directly
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.solana_indexer.public_ip
}