import pandas as pd
import tabula
import requests
import boto3
from database_utils import database_connector

api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

class DataExtractor():
    def __init__(self):
        pass

    def read_rds_table(self, database_connector, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, database_connector.engine)
        #df.to_csv('test.csv')
        print(df)
        return df
    
    def retrieve_pdf_data(self, pdf_link, pdf_local_name):
        response = requests.get(pdf_link)
        with open(f'{pdf_local_name}', 'wb') as file:
            file.write(response.content)
        dfs = tabula.read_pdf(pdf_local_name, stream=True, pages='all')
        df = pd.concat(dfs)
        df.to_csv('card_details_csv.csv') #Save as csv for quicker data cleaning
        return df
    #suppress jpype error here at some point?

    def list_number_of_stores(self, endpoint_url, api_header):
        response = requests.get(endpoint_url, headers=api_header)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint_url, api_header):
        response_list = []
        for i in range(451):
            response = requests.get(f'{endpoint_url}{i}', headers=api_header)
            response_list.append(response.json())

        df = pd.DataFrame(response_list)
        df.to_csv('store_details_csv.csv') #Save as csv for quicker data cleaning
        return df
    
    def extract_from_s3(self, s3_address, file_name):
        s3 = boto3.client('s3')
        bucket_name = s3_address[5:].split('/')[0]
        object_name = '/'.join(s3_address[5:].split('/')[1:])
        s3.download_file(bucket_name, object_name, file_name)
        df = pd.read_csv(file_name)
        return df


    
data_extractor = DataExtractor()
#user_details_df = data_extractor.read_rds_table(database_connector, 'legacy_users')
#card_details_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', 'card_details.pdf')

#number_of_stores = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_header)
#print(number_of_stores)

#store_details_df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', api_header)
#product_details_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv', 'product_details_df.csv')

#order_details_df = data_extractor.read_rds_table(database_connector, 'orders_table')
#order_details_df.to_csv('orders_table.csv')

events_data_json = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json', 'date_details.json')









