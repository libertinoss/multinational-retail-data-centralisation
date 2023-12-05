import pandas as pd
import re
#from data_extraction import user_details_df 
from dateutil.parser import parse
from database_utils import database_connector

class DataCleaning():
    def __init__(self):
        pass

    def clean_user_data(self, df):
        #print(df)
        # "index" row is clearly innaccurate so is dropped to prevent confusion
        df.drop(['index'], axis = 1, inplace = True)
        #print(df)

        # Looking for any strange repeated data 
        for column in df:
            print(df[column].value_counts())

        # Shows there are 21 rows with NULL values that are then removed
        df = df[~(df == 'NULL').any(axis=1)]
        # Also showed GGB mistake which is then corrected
        df['country_code'].replace({'GGB': 'GB'}, inplace=True)
        # Drop all other rows with invalid country codes
        mask_country_code = df['country_code'].str.len() == 2
        df = df[mask_country_code]
        for column in df:
            print(df[column].value_counts()) # Confirms the completely garbled rows are gone in general

        # Check for invalid email addresses
        regex_email = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        print(df['email_address'][~df['email_address'].str.match(regex_email)])
        # Email addresses with double @'s replaced
        df['email_address'] = df['email_address'].str.replace('@@', '@')

        # Show dob's that don't conform to standard YYYY-MM-DD and their index locations
        df.reset_index(drop=True, inplace=True)
        print(df.loc[pd.to_datetime(df['date_of_birth'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_dob_indices = (df.loc[pd.to_datetime(df['date_of_birth'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        # Convert 'date_of_birth' column to datetime using the custom parser and check dates again
        df['date_of_birth'] = df['date_of_birth'].apply(parse)
        print(df.iloc[bad_dob_indices])
        # Repeat for join date 
        print(df.loc[pd.to_datetime(df['join_date'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_jd_indices = (df.loc[pd.to_datetime(df['join_date'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['join_date'] = df['join_date'].apply(parse)
        print(df.iloc[bad_jd_indices])

        #Clean phone numbers by removing "(0)"s then stripping non-digit characters except 'x' and leading '+'
        df['phone_number'] = df['phone_number'].str.replace('(0)', '')
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(r'[^\dx+]', '', x))

        return df
    
    def clean_card_data(self):
        df = pd.read_csv('card_details_csv.csv')
        print(df)

        # First and last row are clearly invalid
        df = df.drop([df.columns[0], df.columns[-1]], axis=1)
        #Checking info which shows a very high amount of null entries in the 'card_number expiry_date' column
        print(df.info())
        # Checking where it is non-null shows that is a combination of card_number and expiry_date, and those respective columns are null for those records
        print(df[df['card_number expiry_date'].isna() == False])

        # Split the values in the 'card_number expiry_date' column and place them into the correct columns
        for index, row in df.iterrows():
            if pd.notna(row['card_number expiry_date']):
                row_split = row['card_number expiry_date'].split()
                df.at[index, 'card_number'] = row_split[0]
                df.at[index, 'expiry_date'] = row_split[1] if len(row_split) > 1 else None
        # 'card_number expiry_date' column is now fine to be dropped
        df = df.drop(df.columns[-1], axis=1)

        # Looking for any strange repeated data
        for column in df:
            print(df[column].value_counts())
        df = df.dropna() #Some null values shown so are dropped

        # Rows with invalid card providers dropped (value counts showed invalid ones only appear once)
        valid_card_providers = {x for x in df['card_provider'] if df['card_provider'].value_counts()[x] > 1}
        df = df[df['card_provider'].isin(valid_card_providers)]

        # Incorrectly formatted payment dates parsed and converted as in clean_user_data method
        df.reset_index(drop=True, inplace=True)
        print(df.loc[pd.to_datetime(df['date_payment_confirmed'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_payment_date_indices = (df.loc[pd.to_datetime(df['date_payment_confirmed'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse)
        print(df.iloc[bad_payment_date_indices])

        # Expiry dates checked to see if they are all MM/YY, and they are
        print(df[~df['expiry_date'].str.match(r'^\d{2}/\d{2}$')])

        # Checking most common card number lengths for each card provider
        df.reset_index(drop=True, inplace=True)
        result = df.groupby('card_provider')['card_number'].apply(lambda x: x.str.len().value_counts())
        print(result)
        # These are then placed in a dictionary and validated against data to display possible nonconforming values
        cardtypes_and_number_of_digits = df.groupby('card_provider')['card_number'].apply(lambda x: x.str.len().value_counts().idxmax()).to_dict()
        for index, row in df.iterrows():
            card_provider = row['card_provider']
            card_number = row['card_number']
            if len(card_number) != cardtypes_and_number_of_digits.get(card_provider):
                print(index, card_provider, card_number)
        ''' - Evidently several card numbers have question marks so but numbers appear correct if they are stripped
            - Aside from that can't infer that the minority of Discovery/Maestro numbers which don't have 16/12 digits are necessarily invalid
            - There is one explicitly invalid VISA 16 digit number that is actually 14 digits at index 13713
            - Originally this was dropped but this was reversed after later seeing valid transactions with in the orders_table'''
        df['card_number'] = df['card_number'].str.replace('?', '', regex=False)
        df.reset_index(drop=True, inplace=True)

        return df
    
    def clean_store_data(self):
        df = pd.read_csv('store_details_csv.csv')
        print(df)

        # Looking for invalid columns
        print(df.info())
        # Drop invalid columns (unnamed, index, lat)
        columns_to_drop = [df.columns[0], df.columns[1], df.columns[4]]
        df = df.drop(columns_to_drop, axis=1)
        # Viewing null records
        print(df[(df.isnull()).any(axis=1)])
        # N/A imputed for null values in first record (webstore) and other rows are dropped
        df.loc[0, ['address', 'longitude', 'latitude', 'locality']] = 'N/A'
        df = df.dropna().reset_index(drop=True)
        print(df)

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            print(df[column].value_counts())

        # Drop invalid country codes
        df = df[df['country_code'].str.len() == 2].reset_index(drop=True)
        # Fix incorrect continent names
        df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace=True)

        # Checking latitudes and longitudes for anything other than digits and a single decimal point (nothing invalid)
        print(df[df['latitude'].str.match(r'^\d+(\.\d+)?$') == False])
        print(df[df['longitude'].str.match(r'^\d+(\.\d+)?$') == False])

        # Checking for any store codes not matching standard pattern (nothing invalid)
        print(df[df['store_code'].str.match(r'^[A-Z]{2}-[0-9A-F]{8}$') == False])

        # Quick check on valid localities and staff numbers (some contain letters but don't necessarily look invalid)
        print(df['locality'].unique())
        print(df['staff_numbers'].unique())

        # Incorrectly formatted opening dates parsed and converted as in other methods
        df.reset_index(drop=True, inplace=True)
        print(df['opening_date'].loc[pd.to_datetime(df['opening_date'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_payment_date_indices = (df.loc[pd.to_datetime(df['opening_date'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['opening_date'] = df['opening_date'].apply(parse)
        print(df['opening_date'].iloc[bad_payment_date_indices])

        return df
    
    def convert_product_weights(self):
        df = pd.read_csv('product_details.csv')

        df = df.dropna().reset_index(drop=True) #Drop null values
        
        # Print unique sets of last two characters across weights to get an idea of different units involved
        print(set([weight[-2:] for weight in df['weight'].unique()]))
        # One rogue value at 1772, '77g .' to be fixed directly, garbled values to be fixed below
        print(df[df['weight'].str[-2:] == ' .'])
        df['weight'] = df['weight'].str.replace('77g .', '77g')

        '''
        - Check to make sure all remaining values are standard real numbers ending in kg, g, ml or oz
        - This displays all the values which are of the form [INTEGER] x [WEIGHT]g (e.g 3 x 100g)
        - For these values the non-numeric characters are stripped and the total weight is calculated and converted to kg
        - For other possibilities the unit is simply stripped from the end and the value is converted to kg
        - A 1:1 estimate is used for ml to gramme
        - Garbled values are dropped
        '''
            
        print(df[df['weight'].str.match(r'^\d+(\.\d+)?(kg|g|ml|oz)$') == False])

        for index, row in df.iterrows():
            weight = row['weight']
            if 'x' in weight:
                weight_split = weight.replace(' ', '').replace('g', '').split('x')
                df.at[index, 'weight'] = round((float(weight_split[0]) * float(weight_split[1]))/1000, 2)    
            elif weight.endswith('kg'):
                df.at[index, 'weight'] = round(float(weight[:-2]), 2)
            elif weight.endswith('ml'):
                df.at[index, 'weight'] = round((float(weight[:-2]) / 1000), 2)
            elif weight.endswith('oz'):
                df.at[index, 'weight'] = round((float(weight[:-2]) * 0.0283495), 2)
            elif weight.endswith('g'):
                df.at[index, 'weight'] = round((float(weight[:-1]) / 1000), 2)
            else:
                df = df.drop(index)

        # Convert weight column to float and rename column for clarity
        df['weight'] = df['weight'].astype(float)
        df = df.rename(columns={'weight': 'weight_kg'})

        return df
    
    def clean_products_data(self, df):
        print(df)
        # Looking for invalid columns
        print(df.info())
        # Drop invalid column 'unnamed'
        df = df.drop(df.columns[0], axis=1)

        # Check for null values, these are all completely invalid records so are dropped
        print(df[(df.isnull()).any(axis=1)])
        df = df.dropna().reset_index(drop=True)

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            print(df[column].value_counts())

        # Strip product prices of pound character and rename column for clarity
        df = df.rename(columns={'product_price': 'product_price_gbp'})
        df['product_price_gbp'] = df['product_price_gbp'].str.replace('£', '')
        # Check for price values which aren't standard real numbers. This shows records with completely garbled values which are dropped
        print(df[df['product_price_gbp'].str.match(r'^\d+(\.\d+)?$') == False])
        df = df[df['product_price_gbp'].str.match(r'^\d+(\.\d+)?$') == True].reset_index(drop=True)

        # Checking for invalid EAN numbers (any characters other than numeric digits), none shown
        print(df[df['EAN'].str.match(r'[^0-9]')])

        # Checking for invalid uuid (anything that doesn't meet standard pattern), none shown
        print(df[~df['uuid'].str.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$')])

        # Dates checked and parsed as previously 
        df.reset_index(drop=True, inplace=True)
        print(df['date_added'].loc[pd.to_datetime(df['date_added'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_date_indices = (df.loc[pd.to_datetime(df['date_added'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['date_added'] = df['date_added'].apply(parse)
        print(df['date_added'].iloc[bad_date_indices])

        return df
    
    def clean_orders_data(self):
        df = pd.read_csv('orders_table.csv')
        print(df)
        # Looking for invalid columns
        print(df.info())
        # Drop invalid columns 'unnamed', 'level_0', 'index', 'first_name', 'last_name', '1'
        columns_to_drop = [df.columns[0], df.columns[1], df.columns[2], df.columns[4], df.columns[5], df.columns[10]]
        df = df.drop(columns_to_drop, axis=1)
        print(df)

        # Quick check to validate store codes and uuids, showing nothing wrong
        print((df['store_code'][df['store_code'].str.match(r'^[A-Z]{2}-[0-9A-F]{8}$') == False]).unique())
        print(df[~df['date_uuid'].str.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$')])
        print(df[~df['user_uuid'].str.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$')])
       
        print(df.dtypes)  # Data types check shows all card numbers and quantities are already integers so nothing that can be inferred invalid
        
        return df
    
    def clean_events_data(self):
        df = pd.read_json('date_details.json')
        print(df)

        # Checking for null values and invalid columns, nothing shown
        print(df.info())

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            print(df[column].value_counts())

        # Checking rows containing 'NULL' string, all invalid so are dropped
        print(df[(df == 'NULL').any(axis=1)])
        df = df[~(df == 'NULL').any(axis=1)].reset_index(drop=True)

        # Check to see which rows don't conform to standard timestamp format
        timestamp_as_datetime = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        print(df[timestamp_as_datetime.isnull()])
        # Those rows are all garbled values so are just dropped completely
        df = df[~timestamp_as_datetime.isnull()].reset_index(drop=True)

        # Checking value counts again shows no problematic values for month, year, day and time_period
        for column in df:
            print(df[column].value_counts())

        # Quick check to validate date_uuid which shows no issues
        print(df[~df['date_uuid'].str.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$')])

        return df

data_cleaning = DataCleaning()
#cleaned_user_data = data_cleaning.clean_user_data(user_details_df)
#database_connector.upload_to_db(df=cleaned_user_data, table_name='dim_users')

cleaned_card_data = data_cleaning.clean_card_data()
database_connector.upload_to_db(df=cleaned_card_data, table_name='dim_card_details')

#cleaned_store_data = data_cleaning.clean_store_data()
#database_connector.upload_to_db(df=cleaned_store_data, table_name='dim_store_details')

#products_data_weights_converted = data_cleaning.convert_product_weights()
#cleaned_products_data = data_cleaning.clean_products_data(products_data_weights_converted)

#database_connector.upload_to_db(df=cleaned_products_data, table_name='dim_products')

#cleaned_orders_data = data_cleaning.clean_orders_data()
#database_connector.upload_to_db(df=cleaned_orders_data, table_name = 'orders_table')

#cleaned_events_data = data_cleaning.clean_events_data()
#database_connector.upload_to_db(df=cleaned_events_data, table_name = 'dim_date_times')






            

        