import pandas as pd
import boto3
from datetime import datetime, timezone
import awswrangler as wr
import logging
import json


logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)


bucket_name = 'vinson-ingestion-zone'

def read_csv_from_s3(bucket_name, file_key):
    """ 
    Uses aws wrangler to read all files from the ingestion buckett and turn them to dataframes.
    Args: bucket_name and file_key.

    Returns: csv files as dataframes

    Raises: 
        logger.error: if the bucket does not exist
    """
    try:
        return wr.s3.read_csv(path=f's3://{bucket_name}/{file_key}')
    except Exception as e:
        logger.error(f"Bucket does not exist")



def return_dataframes(bucket_name):
    """
    Iterates over the table names from the ingestion bucket and puts all dataframes in a list.
    Args: bucket_name.

    Returns: list of dataframes.
    """

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


def data_to_parquet(table, df, bucket):
    """
    Turns all the dataframes to parquet format and puts them in the processed bucket.
    Args: table_name, dataframe, bucket.
    """

    wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/star-schema-{table}.parquet"
    )





# return_dataframes('vinson-ingestion-zone')




#creating dim table from dataframes
def dim_design():
    """
    Iterates through the dataframe list and finds the design dataframe by desired column.
    Filters the required columns.
    Returns: the new dataframe.
    """

    df = return_dataframes(bucket_name)
    for frame in df:
        if 'design_id' in frame.columns:
            required_columns = ["design_id","design_name","file_location", "file_name"]
            dimension_design = frame.filter(required_columns)
            # print(dimension_design)
            return dimension_design



def dim_location():
    """
    Iterates through the dataframe list and finds the location dataframe by desired column.
    Filters the required columns.

    Returns: the new dataframe.
    """
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'address_id' in frame.columns:
            modified_dim_location = frame.rename(columns={'address_id': 'location_id'})
            modified_dim_location.pop('created_at')
            modified_dim_location.pop('last_updated')
            #print(modified_dim_location)
            return modified_dim_location



def staff_df():
    """
    Iterates through the dataframe list and finds the staff dataframe by desired column.
    Filters the required columns.

    Returns: the new dataframe.
    """

    df = return_dataframes(bucket_name)
    for frame in df:
        if 'staff_id' in frame.columns:
            dim_staff_columns = ['staff_id', 'first_name', 'last_name', 'email_address', 'department_id']
            staff_table = frame.filter(dim_staff_columns)
            #print(staff_table)
            return staff_table

def dim_staff():
    """
    Iterates through the dataframe list and finds the department dataframe by desired column.
    Filters the required columns, merges staff and department dataframes. 

    Returns: new dataframe with ordered columns.
    """
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'department_name' in frame.columns:
            department_columns = ['department_id', 'department_name', 'location']
            department_table = frame.filter(department_columns)

            staff_table = staff_df()
            dim_staff_complete = pd.merge(department_table, staff_table,  how='inner', on='department_id')
            dim_staff_complete.pop('department_id')
            columns = dim_staff_complete.columns.tolist()
            new_columns = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
            dim_staff_complete = dim_staff_complete[new_columns]
            #print(dim_staff_complete)
            return dim_staff_complete




def counterparty_table():
    """
    Iterates through the dataframe list and finds the counterparty dataframe by desired column.
    Filters the required columns.

    Returns: the new dataframe.
    """
    df = return_dataframes(bucket_name)
    for frame in df:
        if 'counterparty_id' in frame.columns:
            counterparty_columns = ['legal_address_id', 'counterparty_id', 'counterparty_legal_name']
            counterparty_df = frame.filter(counterparty_columns)
            return counterparty_df



def dim_counterparty():
    """
    Iterates through the dataframe list and finds the address dataframe by desired column.
    Filters the required columns, merges address and counterparty dataframes. 

    Returns: new dataframe with renamed ordered columns.
    """
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
    columns = dim_counterparty_df.columns.tolist()
    updated_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty-legal_address_line_1', 'counterparty-legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']
    dim_counterparty_df = dim_counterparty_df[updated_columns]
    #print(dim_counterparty_df)
    return dim_counterparty_df





def currencies():
    """
    Returns a currency dictionary with currency_code:currency_name key-pair.
    """
    currencies = {
        "AED": "UAE Dirham", "AFN": "Afghani", "ALL": "Lek",
        "AMD": "Armenian Dram", "ANG": "Netherlands Antillian Guilder",
        "AOA": "Kwanza", "ARS": "Argentine Peso", "AUD": "Australian Dollar",
        "AWG": "Aruban Guilder", "AZN": "Azerbaijanian Manat",
        "BAM": "Convertible Marks", "BBD": "Barbados Dollar",
        "BDT": "Taka", "BGN": "Bulgarian Lev", "BHD": "Bahraini Dinar",
        "BIF": "Burundi Franc", "BMD": "Bermudian Dollar",
        "BND": "Brunei Dollar", "BOB": "Boliviano", "BRL": "Brazilian Real",
        "BSD": "Bahamian Dollar", "BTN": "Ngultrum", "BWP": "Pula",
        "BYR": "Belarussian Ruble", "BZD": "Belize Dollar",
        "CAD": "Canadian Dollar", "CDF": "Congolese Franc",
        "CHF": "Swiss Franc", "CLP": "Chilean Peso",
        "CNY": "Yuan Renminbi", "COP": "Colombian Peso",
        "CRC": "Costa Rican Colon", "CUP": "Cuban Peso",
        "CVE": "Cape Verde Escudo", "CZK": "Czech Koruna",
        "DJF": "Djibouti Franc", "DKK": "Danish Krone",
        "DOP": "Dominican Peso", "DZD": "Algerian Dinar",
        "EEK": "Kroon", "EGP": "Egyptian Pound",
        "ERN": "Nakfa", "ETB": "Ethiopian Birr",
        "EUR": "Euro", "FJD": "Fiji Dollar", "FKP": "Falkland Islands Pound",
        "GBP": "Pound Sterling", "GEL": "Lari", "GHS": "Cedi",
        "GIP": "Gibraltar Pound", "GMD": "Dalasi", "GNF": "Guinea Franc",
        "GTQ": "Quetzal", "GYD": "Guyana Dollar", "HKD": "Hong Kong Dollar",
        "HNL": "Lempira", "HRK": "Croatian Kuna", "HTG": "Gourde",
        "HUF": "Forint", "IDR": "Rupiah", "ILS": "New Israeli Sheqel",
        "INR": "Indian Rupee", "IQD": "Iraqi Dinar", "IRR": "Iranian Rial",
        "ISK": "Iceland Krona", "JMD": "Jamaican Dollar",
        "JOD": "Jordanian Dinar", "JPY": "Yen", "KES": "Kenyan Shilling",
        "KGS": "Som", "KHR": "Riel", "KMF": "Comoro Franc",
        "KPW": "North Korean Won", "KRW": "Won", "KWD": "Kuwaiti Dinar",
        "KYD": "Cayman Islands Dollar", "KZT": "Tenge", "LAK": "Kip",
        "LBP": "Lebanese Pound", "LKR": "Sri Lanka Rupee",
        "LRD": "Liberian Dollar", "LSL": "Loti", "LTL": "Lithuanian Litas",
        "LVL": "Latvian Lats", "LYD": "Libyan Dinar",
        "MAD": "Moroccan Dirham", "MDL": "Moldovan Leu",
        "MGA": "Malagasy Ariary", "MKD": "Denar", "MMK": "Kyat",
        "MNT": "Tugrik", "MOP": "Pataca", "MRO": "Ouguiya",
        "MUR": "Mauritius Rupee", "MVR": "Rufiyaa","MWK": "Kwacha",
        "MXN": "Mexican Peso", "MYR": "Malaysian Ringgit",
        "MZN": "Metical", "NAD": "Namibia Dollar", "NGN": "Naira",
        "NIO": "Cordoba Oro", "NOK": "Norwegian Krone", "NPR": "Nepalese Rupee",
        "NZD": "New Zealand Dollar", "OMR": "Rial Omani", "PAB": "Balboa",
        "PEN": "Nuevo Sol", "PGK": "Kina", "PHP": "Philippine Peso",
        "PKR": "Pakistan Rupee", "PLN": "Zloty", "PYG": "Guarani",
        "QAR": "Qatari Rial", "RON": "New Leu", "RSD": "Serbian Dinar",
        "RUB": "Russian Ruble", "RWF": "Rwanda Franc",
        "SAR": "Saudi Riyal", "SBD": "Solomon Islands Dollar",
        "SCR": "Seychelles Rupee", "SDG": "Sudanese Pound",
        "SEK": "Swedish Krona", "SGD": "Singapore Dollar",
        "SHP": "Saint Helena Pound", "SLL": "Leone", "SOS": "Somali Shilling",
        "SRD": "Surinam Dollar", "STD": "Dobra", "SVC": "El Salvador Colon",
        "SYP": "Syrian Pound", "SZL": "Lilangeni", "THB": "Baht",
        "TJS": "Somoni", "TND": "Tunisian Dinar", "TOP": "Paanga",
        "TRY": "Turkish Lira", "TTD": "Trinidad and Tobago Dollar",
        "TWD": "New Taiwan Dollar", "TZS": "Tanzanian Shilling",
        "UAH": "Hryvnia", "UGX": "Uganda Shilling", "USD": "US Dollar",
        "UYU": "Peso Uruguayo", "UZS": "Uzbekistan Sum",
        "VED": "Venezuelan digital bolivar", "VEF": "Bolivar Fuerte",
        "VES": "Bolivar Soberano", "VND": "Dong", "VUV": "Vatu",
        "WST": "Tala", "XAF": "CFA Franc BEAC",
        "XCD": "East Caribbean Dollar", "XOF": "CFA Franc BCEAO",
        "XPF": "CFP Franc", "YER": "Yemeni Rial", "ZAR": "Rand",
        "ZMK": "Zambian Kwacha", "ZWD": "Zimbabwe Dollar"
    }
    return currencies


def dim_currency(return_dataframes_func=return_dataframes, currencies_func=currencies):
    """
    Iterates through the dataframe list and finds the currency dataframe by desired column.
    It creates a dataframe from the currency function, drops unneeded columns and merges with the currency dataframe.
    
    Returns: the new dataframe.
    """    
    df = return_dataframes_func(bucket_name)
    currency_dict = currencies_func()
    for frame in df:
        if 'currency_id' in frame.columns:
            currency_info_df = pd.DataFrame(list(currency_dict.items()), columns=['currency_code', 'currency_name'])
            clean_df = frame.drop(columns=["created_at", "last_updated"])
            currency_df = clean_df.merge(currency_info_df, on='currency_code', how='left')
            # print(currency_df)
            return currency_df


# still need to ask Simon about date dim table
def dim_date():
    """
    Generates a DataFrame containing detailed date information for each day within a specified range of years.

    The function iterates over each day from January 1, 2022, to December 31, 2024, and creates a date object.
    For each valid date, the function extracts various components such as the year, month, day, day of the week, day name,
    month name, and quarter. These components are stored in a list, which is then converted into a DataFrame.
    
    Returns:
        A DataFrame containing detailed date information for each valid day in the specified date range.
    """

    date_result_list = []
    for year in range(2022,2025):
        for month in range(1,13):
            for day in range(1,32):
                try:
                    date_result = pd.to_datetime(f'{year}-{month:02d}-{day:02d}')
                    date_year = int(date_result.strftime('%Y'))
                    date_month = int(date_result.strftime('%m'))
                    date_day = int(date_result.strftime('%d'))
                    day_of_week = int(int(date_result.strftime('%w')) + 1)
                    day_name = date_result.strftime('%A')
                    month_name = date_result.strftime('%B')
                    quarter = (int(date_month)+2)//3
                    date_result_list.append([date_result, date_year, date_month, date_day, day_of_week, day_name, month_name, quarter])
                except ValueError:
                    continue
    dim_date_df = pd.DataFrame(date_result_list,columns=["date_id", 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter'])
    #print(dim_date_df)
    return dim_date_df






def fact_sales_order():
    """
    Iterates through the dataframe list and finds the sales_order dataframe by desired column.
    Filters the required columns, adds sales_record_id columns, renames staff_id columns, 
    and splits created_at and last_updated columns into time and date columns. 

    Returns: new dataframe with ordered columns.
    """
    df = return_dataframes(bucket_name)
    required_columns = ['sales_order_id', 'created_at', 'last_updated', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']
    for frame in df:
        if 'units_sold' in frame.columns:
            fact_sales_order_df = frame.filter(required_columns)
            fact_sales_order_df['sales_record_id'] = range(1, len(fact_sales_order_df) + 1)

            fact_sales_order_df = fact_sales_order_df.rename(columns={'staff_id': 'sales_staff_id'})
            fact_sales_order_df['created_date'] = pd.to_datetime(fact_sales_order_df['created_at']).dt.date
            fact_sales_order_df['created_time'] = pd.to_datetime(fact_sales_order_df['created_at']).dt.time
            fact_sales_order_df['last_updated_date'] = pd.to_datetime(fact_sales_order_df['last_updated']).dt.date
            fact_sales_order_df['last_updated_time'] = pd.to_datetime(fact_sales_order_df['last_updated']).dt.time
            fact_sales_order_df = fact_sales_order_df.drop(columns=["created_at", "last_updated"])

            columns = fact_sales_order_df.columns.tolist()
            ordered_columns = ['sales_record_id', 'sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']
            fact_sales_order_df = fact_sales_order_df[ordered_columns]
            #print(fact_sales_order_df)
            
    return fact_sales_order_df





# dim_design()
# dim_location()
# dim_staff()
# dim_counterparty()
# dim_currency()
# dim_date()
# fact_sales_order()