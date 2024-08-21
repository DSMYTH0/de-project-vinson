import pandas as pd
import boto3
from datetime import datetime
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
            return f"{table}_df"
        #print(df_list[1])
        return df_list
    except Exception as e:
        logger.error(f"Bucket does not exist")
    except:
        logger.error(f"No files found in bucket.")
        print(f"No files found in bucket.")



def populate_schema_tables():
    ingestion_tables = ['design', 'currency', 'staff', 'counterparty', 'address', 'department', 'sales_order']
    df = return_dataframes(bucket_name)
    for frame in df:
        if frame.column in ingestion_tables:
            #do something
    #iterate over df_list
    #if df_list name is equal to table name then populate table and return


def data_to_parquet():



return_dataframes('vinson-ingestion-zone')




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



def dim_counterparty(address_df, counterparty_df):
    merged_counterparty = pd.merge(address_df, counterparty_df, left_on='address_id', right_on='legal_address_id')
    filtered_counterparty = merged_counterparty.filter(
                                            'counterparty_id',
                                            'counterparty_legal_name',
                                            'address_line_1',
                                            'address_line_2',
                                            'district',
                                            'city',
                                            'postal_code',
                                            'country',
                                            'phone'
                                        )
    dim_counterparty_df = filtered_counterparty.rename(columns={
        'counterparty_id' : 'counterparty_id',
        'counterparty_legal_name' : 'counterparty_legal_name',
        'address_line_1' : 'counterparty-legal_address_line_1',
        'address_line_2' : 'counterparty-legal_address_line_2',
        'district' : 'counterparty_legal_district',
        'city' : 'counterparty_legal_city',
        'postal_code' : 'counterparty_legal_postal_code',
        'country' : 'counterparty_legal_country',
        'phone' : 'counterparty_legal_phone_number'
    })
    return dim_counterparty_df

#needs reviewing
def dim_currency(currency_df):
    dim_currency_df = currency_df.filter('currency_id', 'currency_code')
    dim_currency_df['currency_name'] = [currency_name(dim_currency_df['currency_code'])]
    return dim_currency_df




def dim_date():
    date = datetime.now(UTC)
    dim_date_df = pd.DataFrame()
    dim_date_df['date_id'] = [date.date()]
    dim_date_df['year'] = [date.year]
    dim_date_df['month'] = [date.month]
    dim_date_df['day'] = [date.day]
    dim_date_df['day_of_week'] = [pd.to_datetime(date).day_of_week + 1]
    dim_date_df['day_name'] = [pd.to_datetime(date).day_name()]
    dim_date_df['month_name'] = [pd.to_datetime(date).month_name()]
    dim_date_df['quarter'] = [pd.to_datetime(date).quarter]
    return dim_date_df



def fact_sales_order(sales_order_df):
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