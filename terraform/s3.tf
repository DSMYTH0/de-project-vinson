
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "vinson-landing-zone"
}

#Review if bucket_versioning and object_lock actually does what we need them to do

resource "aws_s3_bucket_versioning" "example" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}


resource "aws_s3_bucket_object_lock_configuration" "example" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 20
    }
  }
}



resource "aws_s3_bucket" "code_bucket" {
  bucket = "vinson-code-bucket"
}

