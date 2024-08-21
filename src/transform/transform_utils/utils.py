import pandas as pd
import boto3
from datetime import datetime, timezone
import awswrangler as wr
import logging
import json

# get all table names from ingestion zone
# check the lastest version of all tables ---- ???
# create new table and join neccessary colums 
# change it parque format to get it ready for 

#get csv files from a certain table and concatanate tehm together 




client = boto3.client('s3')

logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)


bucket_name = 'vinson-ingestion-zone'

def read_csv_from_s3(bucket_name, file_key):
    try:
        return wr.s3.read_csv(path=f's3://{bucket_name}/{file_key}')
    except Exception as e:
        logger.error(f"Bucket does not exist")



def return_dataframes(bucket_name):
    try:
        tables = ['address', 'design', 'staff', 'currency', 'counterparty', 'department', 'payment', 'payment_type', 'purchase_order', 'sales_order', 'transaction']
        df_list = []
        for table in tables:
            file_key = table
            df = read_csv_from_s3(bucket_name, file_key)
            df_list.append(df)
        return df_list
    except Exception as e:
        logger.error(f"Bucket does not exist")
    except:
        logger.error(f"No files found in bucket.")
        print(f"No files found in bucket.")


def data_to_parquet():
    pass


return_dataframes('vinson-ingestion-zone')




#creating dim table from dataframes
def dim_design():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'design_id' in frame.columns:
            required_columns = ["design_id","design_name","file_location", "file_name"]
            dimension_design = frame.filter(required_columns)
            #print(dimension_design)
            return dimension_design



def dim_location():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'address_id' in frame.columns:
            modified_dim_location = frame.rename(columns={'address_id': 'location_id'})
            modified_dim_location.pop('created_at')
            modified_dim_location.pop('last_updated')
            #print(modified_dim_location)
            return modified_dim_location



def staff_df():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'staff_id' in frame.columns:
            dim_staff_columns = ['staff_id', 'first_name', 'last_name', 'email_address', 'department_id']
            staff_table = frame.filter(dim_staff_columns)
            return staff_table
            #print(staff_table)

def dim_staff():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'department_name' in frame.columns:
            department_columns = ['department_id', 'department_name', 'location']
            department_table = frame.filter(department_columns)

            staff_table = staff_df()
            dim_staff_complete = pd.merge(department_table, staff_table,  how='inner', on='department_id')
            
            dim_staff_complete.pop('department_id')
            #print(dim_staff_complete)
            return dim_staff_complete




def counterparty_table():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'counterparty_id' in frame.columns:
            counterparty_columns = ['legal_address_id', 'counterparty_id', 'counterparty_legal_name']
            counterparty_df = frame.filter(counterparty_columns)
            return counterparty_df



def dim_counterparty():
    df = return_dataframes(bucket_name)
    counterparty_df = counterparty_table()
    for frame in df:        
        if 'address_id' in frame.columns:
            required_columns = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
            address_df = frame.filter(required_columns)

    merged_counterparty = pd.merge(address_df, counterparty_df, left_on='address_id', right_on='legal_address_id')
    merged_counterparty.pop('legal_address_id')
    merged_counterparty.pop('address_id')

    dim_counterparty_df = merged_counterparty.rename(columns={
        'address_line_1' : 'counterparty-legal_address_line_1',
        'address_line_2' : 'counterparty-legal_address_line_2',
        'district' : 'counterparty_legal_district',
        'city' : 'counterparty_legal_city',
        'postal_code' : 'counterparty_legal_postal_code',
        'country' : 'counterparty_legal_country',
        'phone' : 'counterparty_legal_phone_number'
    })
    #print(dim_counterparty_df)
    return dim_counterparty_df





def get_currency():
    with open("src/transform/transform_utils/currencies.json", "r") as file:
        currency_data = json.load(file)
        return currency_data


def dim_currency():
    df = return_dataframes(bucket_name)
    currency_dict = get_currency()
    for frame in df:
        if 'currency_id' in frame.columns:
            currency_info_df = pd.DataFrame(list(currency_dict['currencies'].items()), columns=['currency_code', 'currency_name'])

            clean_df = frame.drop(columns=["created_at", "last_updated"])

            currency_df = clean_df.merge(currency_info_df, on='currency_code', how='left')
            #print(currency_df)
            return currency_df



def dim_date():
    date = datetime.now(timezone.utc)
    dim_date_df = pd.DataFrame()
    dim_date_df['date_id'] = [date.date()]
    dim_date_df['year'] = [date.year]
    dim_date_df['month'] = [date.month]
    dim_date_df['day'] = [date.day]
    dim_date_df['day_of_week'] = [pd.to_datetime(date).day_of_week + 1]
    dim_date_df['day_name'] = [pd.to_datetime(date).day_name()]
    dim_date_df['month_name'] = [pd.to_datetime(date).month_name()]
    dim_date_df['quarter'] = [pd.to_datetime(date).quarter]
    #print(dim_date_df)
    return dim_date_df





def fact_sales_order(sales_order_df):
    df = return_dataframes(bucket_name)
    # for frame in df:
    #     if:
    #         pass
    fact_sales_order_df = sales_order_df.filter(
        'sales_order_id',
        'staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'design_id',
        'agreed_payment_date',
        'agreed_delivery_date',
        'agreed_deliverty_location_id'
    )
    fact_sales_order_df.rename(columns={'staff_id': 'sales_staff_id'})
    fact_sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at']).date()
    fact_sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at']).time()
    fact_sales_order_df['last_updated_date'] = pd.to_datetime(sales_order_df['last_updated']).date()
    fact_sales_order_df['last_updated_time'] = pd.to_datetime(sales_order_df['last_updated']).time()
    return fact_sales_order_df





dim_design()
dim_location()
dim_staff()
dim_counterparty()
get_currency()
dim_currency()
dim_date()