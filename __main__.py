import pandas as pd
from database_utils import database_connector # Constructor automatically runs methods to read database credentials and initialise a SQL alchemy database engine
from data_extraction import data_extractor, api_header
from data_cleaning import data_cleaning

def extract_user_data():
    database_connector.list_db_tables() # Shows the tables in the database
    user_details_df = data_extractor.read_rds_table(database_connector, 'legacy_users') # Reads user details table and saves as dataframe
    user_details_df.to_csv('extracted_data/user_details.csv') # Save as csv for easier handling and troubleshooting

def extract_card_data():
    # Start with retrieve pdf data function to download pdf, concatenate tables across pages and store in a dataframe
    card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', 'extracted_data/card_details.pdf')
    card_details_df.to_csv('extracted_data/card_details.csv') 

def extract_stores_data():
    # Start with retrieving the number of stores using an API
    number_of_stores = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_header)
    store_details_df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', api_header, number_of_stores)
    store_details_df.to_csv('extracted_data/store_details.csv') 

def extract_product_data():
    data_extractor.extract_from_s3('s3://data-handling-public/products.csv', 'extracted_data/product_details.csv')
    # Function already downloads a csv file so just use this for data cleaning 

def extract_orders_data():
    order_details_df = data_extractor.read_rds_table(database_connector, 'orders_table')
    order_details_df.to_csv('extracted_data/order_details.csv') 

def extract_events_data():
    events_data_json = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json', 'extracted_data/event_details.json')
    # Function already downloads a json that pandas can read so just use this for data cleaning

def clean_and_upload_datasets():
    cleaned_user_data_df = data_cleaning.clean_user_data('extracted_data/user_details.csv')
    cleaned_card_data_df = data_cleaning.clean_card_data('extracted_data/card_details.csv')
    cleaned_store_data_df = data_cleaning.clean_store_data('extracted_data/store_details.csv')
    products_data_weights_converted_df = data_cleaning.convert_product_weights('extracted_data/product_details.csv')
    cleaned_products_data_df = data_cleaning.clean_products_data('extracted_data/product_details_weights_converted.csv') # Function above creates file
    cleaned_orders_data_df = data_cleaning.clean_orders_data('extracted_data/order_details.csv')
    cleaned_events_data_df = data_cleaning.clean_events_data('extracted_data/event_details.json')

    database_connector.upload_to_db(df=cleaned_user_data_df, table_name='dim_users_')  ###############remove underscores at very end
    database_connector.upload_to_db(df=cleaned_card_data_df, table_name='dim_card_details_')
    database_connector.upload_to_db(df=cleaned_store_data_df, table_name='dim_store_details_')
    database_connector.upload_to_db(df=cleaned_products_data_df, table_name='dim_products_')
    database_connector.upload_to_db(df=cleaned_orders_data_df, table_name = 'orders_table_')
    database_connector.upload_to_db(df=cleaned_events_data_df, table_name = 'dim_date_times_')


extract_user_data()
extract_card_data()
extract_stores_data()
extract_product_data()
extract_orders_data()
extract_events_data()
clean_and_upload_datasets()










