For this Friday we need to:

1. Create an ingestion bucket using terraform, 
2. Connect to db using db8000, we should be given read-only access credentials (secrets manager?)
3. write python script for ingesting data
		- Pull all data from table and write to csv(parquet?)
		- there is a table containing all table names
		- AWS lambda with cloudwatch events
		- make it immutable by using datetime to change directory name
		- push to bucket using boto3






**THE EDITH PLAN**


Wed 28th Final Day

AWS - Edith
GitHub - Daniel

Elliot Edith Andrea Svetlana Mohsin Daniel

Admin/Setup:
1. Choose an AWS account and create IAM users
2. Schedule stand-ups and pairings
3. Leaders - who and when?
4. Ticket system - Jira
5. Weaknesses and Strengths?
6. Create a timeline with milestones and deadlines?

Project Structure (Directories/Files):
1. .github/workflows - Contains all the github actions for CI/CD processes:
    deploy.yml: workflow for deployment
2. src - source code
    lambda functions 
    utils functions
3. terraform - configuration files for deploying infrastructure to AWS
    main.tf 
    iam.tf 
    s3.tf etc.
    Cloudwatch
5. tests - contains all test files
6. .gitignore
7. requirements.txt
8. MakeFile - tasks and commands for automation:
    make create-environment
    make dev-setuo
    make requirements
    make test etc.
    
Part 1: Infrastructure Setup
Objective: Set up the AWS infrastructure and configure necessary services.
Tasks:
Terraform:
Configure AWS IAM roles and policies for security.
Create S3 Buckets:
    Set up two S3 buckets: one for ingested data and one for processed data.
    Ensure buckets are structured and organised.
Set Up Job Scheduler:
    Configure AWS EventBridge or Step Functions for job scheduling.
Configure CloudWatch:
    Set up logging and monitoring.
    Configure alerts for failures.
    
Part 2: Data Ingestion
Objective: Develop and deploy a Python application to ingest data from the operational database into S3.
Tasks:
Write a Python application to extract data from the totesys database.
Save data in the ingestion S3 bucket in a suitable format.
Schedule the application to run automatically.
Log progress to CloudWatch and set up email alerts for failures.
Implement measures to prevent SQL injection.

Part 3: Data Transformation
Objective: Develop and deploy a Python application to transform ingested data into a predefined schema for the data warehouse.
Tasks:
Write a Python application to transform ingested data.
Store transformed data in the processed S3 bucket.
Trigger the transformation application upon completion of ingestion jobs.
Log progress and errors to CloudWatch.
Ensure transformed data conforms to the predefined star schema.

Part 4: Data Loading
Objective: Develop and deploy a Python application to load transformed data into the data warehouse.
Tasks:
Write a Python application to load data into the warehouse.
Populate dimension and fact tables of the star schema.
Schedule the application to run at predefined intervals.
Log progress and errors to CloudWatch.

Part 5: Testing and Validation
Objective: Test all components and validate data integrity and performance.
Tasks:
Write tests for all Python applications.
Ensure PEP8 compliance and use safety and bandit for security checks.
Test the entire ETL pipeline end-to-end.
Validate data integrity and schema conformity.
Ensure the system meets the 30-minute data reflection requirement.

Part 6: Documentation and Deployment
Objective: Document the project and deploy the solution using CI/CD techniques.
Tasks:
Write detailed documentation for all components.
Implement infrastructure-as-code for deployment.
Set up CI/CD pipelines for automated deployment and updates.

Part 7: Visualisation
Objective: Create a visual presentation to demonstrate the use of the data warehouse.
Tasks:
Choose a BI dashboard tool like AWS Quicksight, Power BI, Tableau, or Superset.
Create SQL queries to extract useful data.
Build visual elements to display the data.
Prepare a presentation showcasing data insights and usability.



**GROUP PLAN**



**Weakness**

Elliot Edith Andrea Svetlana Mohsin Daniel

Elliot: CI/CD, CW, SQL
Edith: TF, CW
Andrea: CW, testing
Svetlana: testing, CI/CD
Mohsin: CW, error/exception handling
Daniel: CW, CI/CD


**TIMELINE**
Phase One: Project setup
- Repo setup - access
- Requirements
- README
- Setup KANBAN
- Add temp AWS users

Phase Two: Extract
- Get raw data from totesys and land in an s3 bucket
- Establish naming conventions
- Terraform
- Lambda/Utils/ - testing, layers
- Cloudwatch/logging - alerts

Phase Three: Transform
- Clean + manipulate data and place into new s3 bucket
- Establish naming conventions
- Terraform
- Lambda/Utils/ - testing, layers
- Cloudwatch/logging - alerts

Phase Four: Load
- Arrange data into star scheme format ready to land in prepared data warehouse
- Terraform
- Lambda/Utils/ - testing, layers
- Cloudwatch/logging - alerts

Phase Five: Presentation
- Create simple visualisation of project
- SQL queries to answer common business questions


**Project Structure**
- .gitignore
- requirements.txt
- MakeFile - tasks and commands for automation:
    make create-environment
    make dev-setuo
    make requirements
    make test etc.
- Set up Terraform, main, ingestion, IAM
- .github/workflows - Contains all the github actions for CI/CD processes:
    deploy.yml: workflow for deployment
- src - source code
    lambda functions 
    utils functions
- tests - contains all test files






**Phase One: Project setup**
- Repo setup - access
	1. Create repo
	2. Add collaborators access
	3. Create file structure/directories
		- .gitignore
		- makefile
		- requirements.txt
		- src
			- extract/transform/load
		- data
		- tests
			- extract/transform/load
		- terraform
		- README
		- workflows

- Requirements
	- pg8000
	- pytest
	- boto3
	- moto
	- python-dotenv

- README

- Setup KANBAN


- Add temp AWS users
	- get Edith to do it
	- create 5 iam users - admin access
	- use CLI to create config bucket to store tfstate


**Phase Two: Extract**
- Establish naming conventions
	- buckets



- Terraform
	- USE TAGS
	![[Pasted image 20240812112353.png]]
	
	- set up main
		- required providers
		- provider
		- acc details
		- store tfstate in config bucket
	- Create s3 bucket for raw data
	- Create IAM
	- Policy doc -> policy -> attachment
		- Lambda role
			- role needs trust policy
			- putobject access to landing zone bucket
			- cw policy
			- cw metric alarm **

	
- Lambda/Utils/ - testing, layers
	- create connection (using db access) - environment variables
	- pull all data from all tables into mega table using joins

- Cloudwatch/logging - alerts

Use db time data to detect change




**SIMON QUESTIONS**
- When is the best time to set up CI/CD