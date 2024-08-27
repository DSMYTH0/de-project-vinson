Project Overview

The pipeline consists of the following components:

1. Job Scheduler/Orchestration: Utilizes AWS EventBridge to schedule and orchestrate the ingestion and subsequent processing jobs. The scheduler ensures that the data is visible in the data warehouse within 30 minutes of being written to the source database by checking for changes every 15 minutes.


2. S3 Buckets:
    - Ingestion S3 Bucket: Serves as a "landing zone" for raw data ingested from the source database.
    - Processed S3 Bucket: Stores the transformed data, formatted to conform to the data warehouse schema.

3. Python Applications:
    - Extract Lambda: A Python application, deployed on AWS Lambda, checks for changes in the database tables and ingests any new or updated data into the ingestion S3 bucket. It logs status and error messages to AWS CloudWatch, and triggers alerts in case of major errors.

    - Transform Lambda: Another Python application, also running on AWS Lambda, is responsible for transforming the csv files in the ingestion S3 bucket and placing the dimensions and fact tables in parquet format in the processed S3 bucket. It is triggered by S3 events. This application also logs status and error messages to CloudWatch and triggers alerts for any critical issues.

    - Load Lambda: A Python application that periodically updates the data warehouse with the processed data from S3. This application also logs status and error messages to CloudWatch and triggers alerts for any critical issues.

4. Monitoring and Alerts:

    - CloudWatch Logs and Alerts: All applications log their status and errors to AWS CloudWatch. In the event of a major error, CloudWatch triggers an alert that is sent via email.

5. CI/CD:
    - All requirements are automatically run through the yml file and GitHub Actions and automatically deploy Terraform infrastructure.



Acknowledgments
    Special thanks to all the Northcoders mentors and tutors and to our lovely project colleagues.