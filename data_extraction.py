import pandas as pd
import requests
import tabula
import boto3
import json
from tqdm import tqdm


class DataExtractor():
    """    
    This class is used to extract retail data from a variety of sources, with each dataset being saved as
    a pandas dataframe.

    Attributes:
            api_header (dict): API key for authentication
    """    
    def __init__(self):
        with open('api_key.json') as json_data:
            self.api_header = json.load(json_data)
            
    def read_rds_table(self, database_connector, table_name):
        """
        This function is used to read the data from a user selected table in an AWS RDS instance and save
        it in a pandas dataframe.

        Args:
                database_connector(database_utils.DatabaseConnector): Object from database_utils module
                table_name(str): Table from AWS RDS database chosen by user
        Returns:
                df (pandas.DataFrame): Data from the table as a pandas dataframe
        """               
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, database_connector.engine)
            print(f"RDS table {table_name} succesfully read from database")
            return df
        except Exception as e:
            print(f"An error occurred while reading the RDS table: {e}")
            return None
    
    def retrieve_pdf_data(self, pdf_link, pdf_local_name):
        """
        This function is used to download a pdf from the internet and collate the data from each page into
        a single pandas dataframe. Because this process is very lengthy for a long pdf it includes a 
        progress bar which indicates which page it is currently reading in.

        Args:
                pdf_link(str): URL of pdf
                pdf_local_name: Local filepath desired for pdf
        Returns:
                concatenated_df (pandas.DataFrame): Final dataframe concatenated from the dataframes of
                each individual pdf page
        """        
        try:
            response = requests.get(pdf_link)
            with open(f'{pdf_local_name}', 'wb') as file:
                file.write(response.content)
            print(f"{pdf_local_name} successfully downloaded")    

            # Reading pdf to get the list of dataframes representing pages
            pdf_pages = tabula.read_pdf(pdf_local_name, pages='all', guess=False)
            total_pages = len(pdf_pages)

            # Reading pdf with tqdm progress bar
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
        
    def list_number_of_stores(self, endpoint_url):
        """
        This function receives the number of stores from a specified API endpoint.
        Args:
                end_point_url(str): API endpoint for information on number of stores
        Returns:
                mumber_of_stores(int): Number of stores value from json response
        """        
        response = requests.get(endpoint_url, headers=self.api_header)
        if response.status_code == 200: # Successful response
            number_of_stores = response.json().get('number_stores')
            print(f"Successfully connected to API endpoint. Number of stores is {number_of_stores}")
        else:
            print(f"Error: Failed to retrieve data. Status code: {response.status_code}")
            number_of_stores = None
        return number_of_stores
   
    def retrieve_stores_data(self, endpoint_url, number_of_stores):
        """
        This function uses an API to request the json data relating to every store and save it in a
        single pandas dataframe.

        Args:
                end_point_url(str): API endpoint for store details of every store
                number_of_stores(int): Number of stores from json response in list_number_of_stores method
        Returns:
                df (pandas.DataFrame): Collated dataframe of details about every store
                
        """        
        response_list = []    
        for i in range(number_of_stores):
            response = requests.get(f'{endpoint_url}{i}', headers=self.api_header)
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
        """
        This function downloads data from an s3 bucket using a public S3 URI. It splits the URI into the
        name of the S3 bucket and the name of the file within the bucket using a bit of string manipulation
       
        Args:
                s3_address(str): The URI of the desired file to download
                filename(str): Local filepath desired for file
        """   
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
