"""This is the main python file for running the entirety of this multinational data centralisation project. It takes instances of the
DatabaseConnector, DataExtractor, and DataCleaning classes to extract data from the numerous relevant sources, clean each of the
obtained datasets in turn, and then upload all of them to a postgresql database. 

It contains the following functions:
    * extract_user_data - Uses the database_connector object to connect to an Amazon RDS database instance, then the data_extractor to
      save the user data as a csv file 
    * extract_card_data - Uses the data_extractor object to download a pdf and collate the card data from all of its pages into a csv file
    * extract_stores_data - Uses the data_extractor object to get data from each store from their respective API endpoints
      and collate it all into a csv file
    * extract_product_data - Uses the data_extractor object to connect to an s3 bucket and download the product data into a csv file
    * extract_orders_data - Works the same as extract_user_data but for downloading the orders data instead
    * extract_events_data - Works the same as extract_product_data but for downloading a json file of events (when each sale happened)
    * clean_and_upload_datasets - Cleans every dataset in turn and uploads them to separate tables in a postgresql database
"""

import pandas as pd
from database_utils import database_connector # Constructor automatically runs methods to read database credentials and initialise a SQL alchemy database engine
from data_extraction import data_extractor
from data_cleaning import data_cleaning

def extract_user_data():
    database_connector.list_db_tables() # Shows the tables in the database
    user_details_df = data_extractor.read_rds_table(database_connector, 'legacy_users') # Reads user details table and saves as dataframe
    user_details_df.to_csv('extracted_data/user_details.csv') # Save as csv for easier handling and troubleshooting

def extract_card_data():
    # Use retrieve pdf data function to download pdf, concatenate tables across pages and store in a dataframe
    card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', 'extracted_data/card_details.pdf')
    card_details_df.to_csv('extracted_data/card_details.csv') 

def extract_stores_data():
    # Retrieves the number of stores using an API, then use that to retrieve the data from each respective endpoint
    number_of_stores = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
    store_details_df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', number_of_stores)
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

    database_connector.upload_to_db(df=cleaned_user_data_df, table_name='dim_users')  
    database_connector.upload_to_db(df=cleaned_card_data_df, table_name='dim_card_details')
    database_connector.upload_to_db(df=cleaned_store_data_df, table_name='dim_store_details')
    database_connector.upload_to_db(df=cleaned_products_data_df, table_name='dim_products')
    database_connector.upload_to_db(df=cleaned_orders_data_df, table_name = 'orders_table')
    database_connector.upload_to_db(df=cleaned_events_data_df, table_name = 'dim_date_times')

extract_user_data()
extract_card_data()
extract_stores_data()
extract_product_data()
extract_orders_data()
extract_events_data()
clean_and_upload_datasets()










