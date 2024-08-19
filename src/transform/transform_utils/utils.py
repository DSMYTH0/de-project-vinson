import csv
import boto3

client = boto3.client('s3')

# get all table names from ingestion zone
# check the lastest version of all tables
#create new table and join neccessary colums 
# change it parque format to get it ready for 