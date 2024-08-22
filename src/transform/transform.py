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
    except Exception:
        logger.error("Bucket does not exist")



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

def transform_handler(event, context):
    pass

def currencies():
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
    df = return_dataframes_func(bucket_name)
    currency_dict = currencies_func()
    for frame in df:
        if 'currency_id' in frame.columns:
            currency_info_df = pd.DataFrame(list(currency_dict.items()), columns=['currency_code', 'currency_name'])
            clean_df = frame.drop(columns=["created_at", "last_updated"])
            currency_df = clean_df.merge(currency_info_df, on='currency_code', how='left')
            return currency_df
