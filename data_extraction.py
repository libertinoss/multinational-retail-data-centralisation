import pandas as pd
import tabula
import requests
import boto3
from database_utils import database_connector
from tqdm import tqdm
##################################
api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} # put in constructor later #

class DataExtractor():
    def __init__(self):
        pass

    def read_rds_table(self, database_connector, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, database_connector.engine)
            print(f"RDS table {table_name} succesfully read from database")
            return df
        except Exception as e:
            print(f"An error occurred while reading the RDS table: {e}")
            return None
    
    def retrieve_pdf_data(self, pdf_link, pdf_local_name):
        try:
            response = requests.get(pdf_link)
            with open(f'{pdf_local_name}', 'wb') as file:
                file.write(response.content)
            print(f"{pdf_local_name} successfully downloaded")    

            # Reading PDF to get the list of DataFrames representing pages
            pdf_pages = tabula.read_pdf(pdf_local_name, pages='all', guess=False)
            total_pages = len(pdf_pages)

            # Reading PDF with tqdm progress bar
            dfs = []
            for i in tqdm(range(1, total_pages + 1), desc=f"Reading {pdf_local_name}", unit=" page"):
                df = tabula.read_pdf(pdf_local_name, stream=True, pages=i)
                dfs.extend(df)

            concatenated_df = pd.concat(dfs)
            print(f"All pages of {pdf_local_name} successfully read and concatenated into a single dataframe")
            return concatenated_df
        
        except requests.RequestException as re:
            print(f"RequestException: {re}")
            return None
        
        except tabula.errors.JavaNotFoundError as je:
            print(f"JavaNotFoundError: {je}")
            return None
       
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    def list_number_of_stores(self, endpoint_url, api_header):
            response = requests.get(endpoint_url, headers=api_header)
            if response.status_code == 200: # Successful response
                number_of_stores = response.json().get('number_stores')
                print(f"Successfully connected to API endpoint. Number of stores is {number_of_stores}")
            else:
                print(f"Error: Failed to retrieve data. Status code: {response.status_code}")
                number_of_stores = None
            return number_of_stores
   
    
    def retrieve_stores_data(self, endpoint_url, api_header, number_of_stores):
        response_list = []    
        for i in range(number_of_stores):
            response = requests.get(f'{endpoint_url}{i}', headers=api_header)
            if response.status_code == 200:
                response_list.append(response.json())
                print(f"Collected data from store {i} of {number_of_stores}", end='\r')
            else:
                print(f"Failed to retrieve data for store {i}. Status code: {response.status_code}")

        if response_list:
            df = pd.DataFrame(response_list)
            print("Successfully created data frame of stores data from collated json responses")
            return df
        else:
            print("Failed to create dataframe, no data received")
            return None

 
    def extract_from_s3(self, s3_address, file_name):
        try:
            s3 = boto3.client('s3')
            bucket_name = s3_address[5:].split('/')[0]
            object_name = '/'.join(s3_address[5:].split('/')[1:])
            
            # Attempt to download the file from S3
            s3.download_file(bucket_name, object_name, file_name)
            print(f"Succesfully downloaded {object_name} from S3 as {file_name}")

        except Exception as ex:
            print(f"Error downloading file from S3: {ex}")
            return None


    
data_extractor = DataExtractor()
#user_details_df = data_extractor.read_rds_table(database_connector, 'legacy_users')
#card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', 'card_details.pdf')

#number_of_stores = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_header)
#print(number_of_stores)

#store_details_df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', api_header)
#product_details_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv', 'extracted_data/product_details_df.csv')

#order_details_df = data_extractor.read_rds_table(database_connector, 'orders_table')
#order_details_df.to_csv('orders_table.csv')

#events_data_json = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json', 'date_details.json')









