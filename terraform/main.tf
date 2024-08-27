
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "test-project-config-bucket"
    key = "terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "Project Vinson"
      Team = "Vinson Team"
      DeployedFrom = "Terraform"
      Repository = "de-project-vinson"
      CostCentre = "DE"
      Environment = "dev"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}