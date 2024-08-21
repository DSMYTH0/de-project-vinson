import pandas as pd
import boto3
from datetime import datetime, timezone
import awswrangler as wr
import logging

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
        # tables = fetch_table_names(conn)
        tables = ['address', 'design', 'staff', 'currency', 'counterparty', 'department', 'payment', 'payment_type', 'purchase_order', 'sales_order', 'transaction']
        # print(tables)
        df_list = []
        for table in tables:
            file_key = table
            df = read_csv_from_s3(bucket_name, file_key)
            df_list.append(df)
            #return f"{table}_df"
        #print(df_list)
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

dim_design()

def dim_location():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'address_id' in frame.columns:
            modified_dim_location = frame.rename(columns={'address_id': 'location_id'})
            modified_dim_location.pop('created_at')
            modified_dim_location.pop('last_updated')
            #print(modified_dim_location)
            return modified_dim_location

dim_location()

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


dim_staff()

def counterparty_table():
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'counterparty_id' in frame.columns:
            counterparty_columns = ['legal_address_id', 'counterparty_id', 'counterparty_legal_name']
            counterparty_df = frame.filter(counterparty_columns)
            #print(counterparty_df)
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


dim_counterparty()

#needs reviewing
def dim_currency(currency_df):
    dim_currency_df = currency_df.filter('currency_id', 'currency_code')
    dim_currency_df['currency_name'] = [currency_name(dim_currency_df['currency_code'])]
    return dim_currency_df




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
    print(dim_date_df)
    return dim_date_df

dim_date()

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