import pandas as pd
import boto3
from datetime import datetime

# get all table names from ingestion zone
# check the lastest version of all tables
#create new table and join neccessary colums 
# change it parque format to get it ready for 

#get csv files from a certain table and concatanate tehm together 
current_date = datetime.now()

client = boto3.client('s3')
bucket_name = 'vinson-ingestion-zone'
response = client.list_objects_v2(Bucket=bucket_name, Prefix=f'design/Year-{current_date.year}/Month-{current_date.month}/Day-{current_date.day}')
files = [content['Key'] for content in response.get('Contents', [])]
print(files)
dataframe_1 = []
for file_key in files:
    df = pd.read_csv(f's3://{bucket_name}/{file_key}')
    dataframe_1.append(df)
combined_df = pd.concat([dataframe_1[0], dataframe_1[1]])
drop_duplicates = combined_df.drop_duplicates(inplace=False)
print(drop_duplicates)

#creating dim table from dataframes
def dim_design(design_df):
    required_columns = ["design_id","design_name","file_location", "file_name"]
    dimension_design = design_df.filter(required_columns)
    return dimension_design

def dim_location(address_df):
    modified_dim_location = address_df.rename(columns={'address_id': 'location_id'})
    modified_dim_location.pop('created_at')
    modified_dim_location.pop('last_updated')
    return modified_dim_location

def dim_staff(staff_df, department_df):
    dim_staff_columns = ['staff_id', 'first_name', 'last_name', 'email_address']
    dim_staff['department_name'] = department_df['department_name']
    dim_staff['location'] = department_df['location']
    merged_tables = pd.merge(staff_df, department_df, ON=['department_id'], how='inner')
    dim_staff_complete = merged_tables.filter('staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location')
    return dim_staff_complete