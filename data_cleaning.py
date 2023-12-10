import pandas as pd
import re
#from data_extraction import user_details_df 
from dateutil.parser import parse
from database_utils import database_connector

def xprint(*args, **kwargs): #################### explain this later
    if __name__ == "__main__":
        print(*args, **kwargs)

class DataCleaning():
    def __init__(self):
        pass

    def clean_user_data(self, file):

        df = pd.read_csv(file)
        xprint(df)
        # Index and unnamed row are unnecessary so dropped to prevent confusion
        df = df.drop([df.columns[0], df.columns[1]], axis=1)
        # Check for rows with null values shows 21 across all columns. These are clearly invalid records so are dropped
        if __name__ == "__main__":
            print(df.info())
        xprint(' \n Null records: \n', df[df.isnull().any(axis=1)])
        df = df.dropna(axis=0).reset_index(drop=True)

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            xprint(df[column].value_counts())

        # Looking at categorical columns showed there are clearly records with garbled values
        # Also showed that some country codes written as GGB instead of GB
        df['country_code']= df['country_code'].str.replace('GGB', 'GB') # GGB corrected
        xprint('\n Garbled records: \n', df[df['country_code'].str.len() != 2]) # Looking at invalid records (all country codes should be 2)
        # All of those records were evidently completely invalid across the board so are dropped
        df = df[df['country_code'].str.len() == 2].reset_index(drop=True)

        # Define regex patterns to do basic validation on name columns, email_address and uuid
        name_pattern = r'^[\w\s-]*$' # Match alphabetic characters, spaces, hyphens
        email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$' # Standard email regex
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' # Standard uuid regex

        # Check for invalid first and last names, only ones shown contain fullstops and are not invalid so no action taken
        xprint('\n Regex nonconforming names: \n', df[['first_name', 'last_name', 'address']][~(df['first_name'].str.match(name_pattern) | ~df['last_name'].str.match(name_pattern))])    
        # Check for invalid email addresses
        xprint('\n Regex nonconforming email addresses: \n', df['email_address'][~df['email_address'].str.match(email_pattern)])
        # All email addresses shown have double @'s which are corrected
        df['email_address'] = df['email_address'].str.replace('@@','@')
        # Check for invalid uuids shows none
        xprint('\n Regex nonconforming uuids: \n', df['user_uuid'][~df['user_uuid'].str.match(uuid_pattern)])

        # Show dob's that don't conform to standard YYYY-MM-DD and their index locations
        xprint('\n Nonconforming DOBs: \n', df['date_of_birth'][pd.to_datetime(df['date_of_birth'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_dob_indices = (df.loc[pd.to_datetime(df['date_of_birth'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        # Convert 'dxate_of_birth' column to datetime using the custom parser and check dates again - all look good
        df['date_of_birth'] = df['date_of_birth'].apply(parse)
        xprint('\n Parsed DOBs: \n', df.iloc[bad_dob_indices]['date_of_birth'])

        # Repeat same process for join date 
        xprint('\n Nonconforming join dates: \n', df['join_date'][pd.to_datetime(df['join_date'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_jd_indices = (df.loc[pd.to_datetime(df['join_date'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['join_date'] = df['join_date'].apply(parse)
        xprint('\n Parsed join dates: \n',df.iloc[bad_jd_indices]['join_date'])

        # Too many variations of phone numbers for sensible validation, but worth standardising
        # Cleaned by removing "(0)"s then stripping non-digit characters except 'x' and leading '+'
        df['phone_number'] = df['phone_number'].str.replace('(0)', '')
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(r'[^\dx+]', '', x))

        print("User data successfully cleaned")
        return df
    
    def clean_card_data(self, file):
        
        df = pd.read_csv(file)
        xprint(df)
        # First and last columns are clearly invalid so dropped to prevent confusion
        df = df.drop([df.columns[0], df.columns[-1]], axis=1)
        # Checking info which shows a very high amount of null entries in the 'card_number expiry_date' column
        if __name__ == "__main__":
            print(df.info())
        # Checking where it is non-null shows that is a combination of card_number and expiry_date, and those respective columns are null for those records
        xprint('\n Records with non-null "card_number expiry_date"  \n', df[df['card_number expiry_date'].isnull() == False])

        # Split the values in the 'card_number expiry_date' column and place them into the correct columns
        for index, row in df.iterrows():
            if pd.notna(row['card_number expiry_date']):
                row_split = row['card_number expiry_date'].split()
                df.at[index, 'card_number'] = row_split[0]
                df.at[index, 'expiry_date'] = row_split[1] if len(row_split) > 1 else None
        # 'card_number expiry_date' column is now fine to be dropped
        df = df.drop(df.columns[-1], axis=1)

        # Other records containing any null values checked, clearly invalid so are dropped
        xprint(' \n Null records: \n', df[df.isnull().any(axis=1)])
        df = df.dropna(axis=0).reset_index(drop=True)

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            xprint(df[column].value_counts())

        # Looking at categorical columns showed there are clearly records with garbled values
        # Start with dropping all records with invalid card providers (which evidently only appeared in value counts only once)
        valid_card_providers = {x for x in df['card_provider'] if df['card_provider'].value_counts()[x] > 1}
        df = df[df['card_provider'].isin(valid_card_providers)].reset_index(drop=True)

        # Incorrectly formatted payment dates parsed and converted as in clean_user_data method
        xprint('\n Nonconforming payment dates: \n', df['date_payment_confirmed'][pd.to_datetime(df['date_payment_confirmed'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_payment_date_indices = (df.loc[pd.to_datetime(df['date_payment_confirmed'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse)
        xprint('\n Parsed payment dates: \n',df.iloc[bad_payment_date_indices]['date_payment_confirmed'])

        # Expiry dates checked to see if they are all MM/YY, and they are
        xprint(df[~df['expiry_date'].str.match(r'^\d{2}/\d{2}$')])

        # Checking most common card number lengths for each card provider
        df.reset_index(drop=True, inplace=True)
        xprint('\n Card provider - Card Length - Number of records \n', df.groupby('card_provider')['card_number'].apply(lambda x: x.str.len().value_counts()))
        # The most common card number length from each is placed in a dictionary and validated against data to display possible nonconforming values
        cardtypes_and_number_of_digits = df.groupby('card_provider')['card_number'].apply(lambda x: x.str.len().value_counts().idxmax()).to_dict()
        xprint('\n Possible non-conforming card numbers: \n')
        for index, row in df.iterrows():
            card_provider = row['card_provider']
            card_number = row['card_number']
            if len(card_number) != cardtypes_and_number_of_digits.get(card_provider):
                xprint(index, card_provider, card_number)
                
        ''' - Evidently several card numbers have question marks but numbers are correct when they are stripped of these and the code run again
            - Aside from that can't infer that the minority of Discovery/Maestro numbers which don't have 16/12 digits are necessarily invalid
            - There is one explicitly invalid VISA 16 digit number that is actually 14 digits at index 13713 (card no. 46026611441111)
            - Originally this was dropped but this was reversed after later seeing valid transactions with it in the orders_table which imply it is somehow...valid
        '''

        df['card_number'] = df['card_number'].str.replace('?', '')

        print("Card data successfully cleaned")
        return df
    
    def clean_store_data(self, file):

        df = pd.read_csv(file)
        pd.set_option('display.max_columns', None)
        xprint(df)
        # Unnamed and index columns unnecessary to to be dropped
        # lat column appears invalid, quick check for the unique values it contains confirms only nulls and garbled values
        # These three columns are therefore dropped 
        xprint(df['lat'].unique())
        columns_to_drop = [df.columns[0], df.columns[1], df.columns[4]]
        df = df.drop(columns_to_drop, axis=1)

        # Checking info appears to show several rows with null values, and one more than the rest for 'address', 'longitude', 'latitude', 'locality'
        if __name__ == "__main__":
            print(df.info())
        # Viewing null records
        xprint(' \n Null records: \n', df[df.isnull().any(axis=1)])
        # We can see one of these is the first record (webstore),
        # 'N/A' imputed for null values in first record (webstore) as the rest of the record is valid and these values understandably have to be N/A
        # The rest of the records are all completely null so are dropped
        df.loc[0, ['address', 'longitude', 'latitude', 'locality']] = 'N/A'
        df = df.dropna().reset_index(drop=True)

        # Looking for any strange repeated data and outliers in categorical columns
        for column in df:
            xprint(df[column].value_counts())

        # Looking at categorical columns showed there are clearly records with garbled values
        # Also showed that some continent names written as eeAmerica and eeEurope incorrectly
        df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace=True) # Continent names corrected
        xprint('\n Garbled records: \n', df[df['country_code'].str.len() != 2]) # Looking at invalid records (all country codes should be 2)
        # All of those records were evidently completely invalid across the board so are dropped
        df = df[df['country_code'].str.len() == 2].reset_index(drop=True)

        # Define regex patterns to do basic validation on latitude/longtitude, and store_code
        lat_long_pattern = r'^-?\d+(\.\d+)?$' # Matches decimal numbers (just digits, starting minus, single decimal point allowed)
        store_code_pattern = r'^[A-Z]{2}-[0-9A-F]{8}$' # Matches the standard store code pattern (XX-XXXXXXXX) where first two chars are uppercase letters and next eight chars are digits/uppercase letters

        # Checking nonconforming latitudes and longitudes shows none
        xprint('Regex nonconforming latitudes and longitudes')
        xprint(df['latitude'][~df['latitude'].str.match(lat_long_pattern)])
        xprint(df['longitude'][~df['longitude'].str.match(lat_long_pattern)])
        # Checking nonconforming store_codes shows none aside from webstore (which is fine)
        xprint('\n Regex nonconforming store codes \n', df['store_code'][~df['store_code'].str.match(store_code_pattern)])

        # Dataset is relatively small so can quickly look over unique values for locality and staff numbers to look for anything obviously invalid
        xprint('\n Unique localities: \n', df['locality'].unique())
        xprint('\n Unique values for staff numbers: \n', df['staff_numbers'].unique())
        # Some values for staff numbers contain letters so the associated records are investigated
        xprint('\n Records with nonconforming staff numbers: \n', df[pd.to_numeric(df['staff_numbers'], errors='coerce').isnull()])
        # There doesn't appear to be anything else invalid about the above records
        # It seems inadvisable to delete records of otherwise entirely valid stores so benefit of the doubt is given and non-numeric characters are simply stripped from these records
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub(r'\D', '', x))

        # Incorrectly formatted opening dates parsed and converted as in other methods
        xprint('\n Nonconforming opening dates: \n', df['opening_date'][pd.to_datetime(df['opening_date'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_opening_date_indices = (df.loc[pd.to_datetime(df['opening_date'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['opening_date'] = df['opening_date'].apply(parse)
        xprint('\n Parsed opening dates: \n', df.iloc[bad_opening_date_indices]['opening_date'])

        print("Store data successfully cleaned")
        return df
    
    def convert_product_weights(self, file):
        
        df = pd.read_csv(file)
        pd.set_option('display.max_columns', None)
        # Check for records with null values that would mess with method before beginning conversion
        xprint(' \n Null records: \n', df[df.isnull().any(axis=1)])
        df = df.dropna().reset_index(drop=True)

        # Print unique sets of last two characters across weights to get an idea of different units involved
        possible_weight_units = list(set([weight[-2:] for weight in df['weight'].unique()]))
        xprint('\n Unique last two chars of weight values \n',possible_weight_units)
        # This shows possible units as kg, g, ml, oz and some other strange values
        # The records with other values are investigated further
        invalid_units = [unit for unit in possible_weight_units if unit not in ['kg', 'ml', 'oz'] and unit[-1] != 'g']
        xprint('\n Records with invalid weight units \n', df[df['weight'].str[-2:].isin(invalid_units)])
        # This shows 4 completed garbled and a rogue weight value of '77g .'
        # Start with fixing the rogue value directly
        df['weight'] = df['weight'].str.replace('77g .', '77g')

        '''
        - Check to make sure all remaining values are standard real numbers ending in kg, g, ml or oz
        - This displays all the values which are of the form [INTEGER] x [WEIGHT]g (e.g 3 x 100g)
        - For these values the non-numeric characters are stripped and the total weight is calculated and converted to kg
        - For other possibilities the unit is simply stripped from the end and the value is converted to kg
        - A 1:1 estimate is used for ml to gramme
        - Garbled values (not matching one of the standard units) are dropped
        '''
            
        xprint('\n Nonstandard weight values \n', df['weight'][~df['weight'].str.match(r'^\d+(\.\d+)?(kg|g|ml|oz)$')])

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
        df.to_csv('extracted_data/product_details_weights_converted.csv')
        
        print("Weights in product data successfully standardised to kg")
        return df
    
    def clean_products_data(self, file):

        df = pd.read_csv(file)
        pd.set_option('display.max_columns', None)
        xprint(df)
        # First two unnamed columns unncecessary so are dropped
        df = df.drop([df.columns[0], df.columns[1]], axis=1)
        # Check info for null values but none shown
        if __name__ == "__main__":
            print(df.info())

        # Looking for any strange repeated data and outliers in categorical columns
        # Doesn't show any obvious invalid data
        for column in df:
            xprint(df[column].value_counts())

        # Strip product prices of pound character and rename column for clarity
        df = df.rename(columns={'product_price': 'product_price_gbp'})
        df['product_price_gbp'] = df['product_price_gbp'].str.replace('£', '')

        # Define regex patterns to do basic validation on product price, uuid and product code
        product_price_pattern = r'^\d+(\.\d{2})?$' # Matches decimal numbers for prices (just digits with a decimal point and explicitly two digits after it)
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' # Standard uuid regex
        product_code_pattern = r'^[a-zA-Z][0-9]-[a-zA-Z0-9]*$' # Regex for apparent product code pattern, alphabetic first char, numeric second char, then hyphen followed by string of alphanumeric characters

        # Check for invalid product prices shows none
        xprint('\n Regex nonconforming product prices: \n', df[['product_price_gbp']][~df['product_price_gbp'].str.match(product_price_pattern)]) 
        # Check for invalid uuids shows none
        xprint('\n Regex nonconforming uuids: \n', df[['uuid']][~df['uuid'].str.match(uuid_pattern)]) 
        # Check for invalid product codes shows none
        xprint('\n Regex nonconforming product codes: \n', df[['product_code']][~df['product_code'].str.match(product_code_pattern)])

        # Dates checked and parsed as previously 
        xprint('\n Nonconforming dates: \n', df['date_added'][pd.to_datetime(df['date_added'], errors='coerce',format='%Y-%m-%d').isnull()])
        bad_date_added_indices = (df.loc[pd.to_datetime(df['date_added'], errors='coerce',format='%Y-%m-%d').isnull()]).index
        df['date_added'] = df['date_added'].apply(parse)
        xprint('\n Parsed dates: \n', df.iloc[bad_date_added_indices]['date_added'])

        print("Products data successfully cleaned")
        return df
    
    def clean_orders_data(self, file):

        df = pd.read_csv(file)
        pd.set_option('display.max_columns', None)
        xprint(df)
        if __name__ == "__main__":
            print(df.info())
        # 'unnamed', 'level_0', 'index', 'first_name', 'last_name', '1' all look to be invalid columns so are dropped
        columns_to_drop = [df.columns[0], df.columns[1], df.columns[2], df.columns[4], df.columns[5], df.columns[10]]
        df = df.drop(columns_to_drop, axis=1)

        # Define regex patterns to do basic validation on uuids, store codes and product codes
        # Info already showed card numbers and quantities are already integers so nothing that can be inferred invalid there
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' # Standard uuid regex
        store_code_pattern = r'^[A-Z]{2}-[0-9A-F]{8}$' # Matches the standard store code pattern (XX-XXXXXXXX) where first two chars are uppercase letters and next eight chars are digits/uppercase letters
        product_code_pattern = r'^[a-zA-Z][0-9]-[a-zA-Z0-9]*$' # Regex for apparent product code pattern, alphabetic first char, numeric second char, then hyphen followed by string of alphanumeric characters

        # Check for invalid uuids shows none
        xprint('\n Regex nonconforming uuids: \n', df[['date_uuid', 'user_uuid']][~(df['date_uuid'].str.match(uuid_pattern) | ~df['user_uuid'].str.match(uuid_pattern))]) 
        # Check for invalid store codes shows none
        xprint('\n Regex nonconforming store codes: \n', df[['store_code']][~(df['store_code'].str.match(store_code_pattern)) & (df['store_code'] != 'WEB-1388012W')])
        # Check for invalid product codes shows none
        xprint('\n Regex nonconforming product codes: \n', df[['product_code']][~df['product_code'].str.match(product_code_pattern)])
        
        print("Orders data successfully cleaned")
        return df
    
    def clean_events_data(self, file):

        df = pd.read_json(file)
        # Printing df and looking at info doesn't show any invalid columns or null values
        xprint(df)
        if __name__ == "__main__":
            print(df.info())

        # Looking for any strange repeated data and outliers in categorical columns
        # This showed several rows containing 'NULL' string
        for column in df:
            xprint(df[column].value_counts())

        # Investigating these rows shows them all to be completely invalid so are dropped 
        xprint('\n, Rows with NULL: \n', df[(df == 'NULL').any(axis=1)])
        df = df[~(df == 'NULL').any(axis=1)].reset_index(drop=True)

        # Check to see which rows don't conform to standard timestamp format
        timestamp_as_datetime = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        xprint('\n Invalid timestamps: \n', df[timestamp_as_datetime.isnull()])
        # Those rows are all garbled values so are just dropped completely
        df = df[~timestamp_as_datetime.isnull()].reset_index(drop=True)

        # Checking value counts again shows no problematic values for month, year, day and time_period
        for column in df:
            xprint(df[column].value_counts())

        # Quick check to validate date_uuid which shows nothing invalid
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' # Standard uuid regex
        xprint('\n Regex nonconforming uuids: \n', df[['date_uuid']][~df['date_uuid'].str.match(uuid_pattern)]) 

        print("Events data successfully cleaned")
        return df

data_cleaning = DataCleaning()






            

        