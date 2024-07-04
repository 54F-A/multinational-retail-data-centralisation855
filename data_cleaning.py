from data_extraction import DataExtractor
import database_utils
import datetime
import pandas as pd
import re
import uuid


class DataCleaning:
    """A class to clean and preprocess user data extracted from a database.

    Attributes:
        data (DataFrame): The DataFrame containing user data extracted from the database.
    """

    def __init__(self, data):
        """Initialises the DataCleaning instance with the provided DataFrame.

        Args:
            data (DataFrame): The initial DataFrame containing user data extracted from the database.
        """
        self.data = data
    
    def clean_user_data(self):
        """Cleans the user data.

        Performs the following:
        1.Converts the date_of_birth & join_date columns to datetime format.
        2. Maps country_code to country.
        3. Formats phone number depending on country_code.

        Returns:
            DataFrame: Cleaned DataFrame containing processed user data.
        """
        df = self.data.copy()

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        country_mapping = {
            'Germany': 'DE',
            'United Kingdom': 'GB',
            'United States': 'US'
        }
        df['country_code'] = df['country'].map(country_mapping)
        df = df[df['country_code'].notna()]


        def format_phone_number(phone_number, country_code):
            """Converts product weight to kg.

            Args:
                phone_number (DataFrame): DataFrame containing product data with weight column.
                country_code

            Returns:
                DataFrame: Cleaned DataFrame with weights converted to kg.
            """
            phone_number = re.sub(r'\s+|\(|\)|\.|-', '', phone_number)

            if country_code == 'GB': 
                if phone_number.startswith('('):
                    return f"({phone_number[1:6]} {phone_number[6:9]} {phone_number[9:]}"
                elif phone_number.startswith('0'):
                    return f"{phone_number[0:5]} {phone_number[5:]}"
                elif phone_number.startswith('+44'):
                    if len(phone_number) == 13:
                        return f"{phone_number[0:3]} {phone_number[3:7]} {phone_number[7:]}"
                    elif len(phone_number) == 14:
                        phone_number = phone_number.replace('+44', '')
                        return f"{phone_number[0:3]} {phone_number[3:7]} {phone_number[7:]}"
                else:
                    return phone_number
            
            elif country_code == 'US':
                if 'x' in phone_number:
                    return f"{phone_number[0:3]} {phone_number[3:6]} {phone_number[6:10]} {phone_number[10:]}"
                elif phone_number.startswith('+'):
                    if len(phone_number) > 10:
                        return f"{phone_number[0:3]} {phone_number[3:6]} {phone_number[6:10]} {phone_number[10:]}"
                elif phone_number.startswith('00'):
                    phone_number = phone_number.replace('00', '', 1)
                    return f"+{phone_number[0:1]} {phone_number[1:5]} {phone_number[5:8]} {phone_number[8:]}"
                else:
                    return phone_number
                
            elif country_code == 'DE':
                if phone_number.startswith('+49'):
                    return f"{phone_number[0:3]} {phone_number[3:5]} {phone_number[5:9]} {phone_number[9:]}"
                elif phone_number.startswith('0'):
                    return f"{phone_number[0:4]} {phone_number[4:]}"
                else:
                    return phone_number
            
            else:
                return phone_number

        df = df[df['phone_number'].notna()]
        df['phone_number'] = df.apply(lambda row: format_phone_number(row['phone_number'], row['country_code']), axis=1)
        df.reset_index(drop=True, inplace=True)
        self.data = df
        
        return df
    
    def clean_card_data(self):
        """Cleans the card data.

        Performs the following:
        1.

        Returns:
            DataFrame: Cleaned DataFrame containing processed card data.
        """
        df = self.data.copy()

        valid_expiry_date = re.compile(r'^(0[1-9]|1[0-2])\/\d{2}$')
        df = df[df['expiry_date'].apply(lambda x: bool(valid_expiry_date.match(x)))]

        df['card_number'] = df['card_number'].astype(str)
        df = df[df['card_number'].notna()]
        df['card_number'] = df['card_number'].str.replace('?', '')
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df

    def clean_store_data(self):
        """Cleans the store data.

        Performs the following:
        1.

        Returns:
            DataFrame: Cleaned DataFrame containing processed store data.
        """
        df = self.data.copy()

        df = df[df['longitude'].astype(str).str.match(r'^\d+(\.\d+)?$|^N/A$')]
        columns_to_remove = ['lat']
        df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
        df.insert(3, 'latitude', df.pop('latitude'))
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df

    def convert_product_weights(self, products_df):
        """Converts product weight to kg.

        Args:
            products_df (DataFrame): DataFrame containing product data with weight column.

        Returns:
            DataFrame: Cleaned DataFrame with weights converted to kg.
        """
        def convert_weight(weight):
            if isinstance(weight, str):
                weight = weight.lower().strip()
                conversions = {
                    'kg': 1,
                    'g': 0.001,
                }

                for unit, factor in conversions.items():
                    if unit in weight:
                        numeric_value = float(re.sub(r'[^\d.]', '', weight))
                        return f"{round(numeric_value * factor, 3)} kg"
        
        products_df['weight'] = products_df['weight'].apply(convert_weight)
        products_df.reset_index(drop=True, inplace=True)

        return products_df
    
    def clean_products_data(self, products_df):
        """Cleans the product data.

        Args:
            products_df (DataFrame): DataFrame containing product data.

        Returns:
            DataFrame: Cleaned DataFrame with erroneous values removed.
        """
        df = products_df.copy()

        df.dropna(inplace=True)
        df = df.loc[df['removed'].isin(['Still_avaliable', 'Removed'])]
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)       
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df
    
    def clean_date_times_data(self):
        """Cleans the date details data.

        Performs the following:
        1. Drops rows with missing values.

        Returns:
            DataFrame: Cleaned DataFrame containing processed date details data.
        """
        df = self.data.copy()

        df.dropna(inplace=True) 

        def is_valid_uuid(val):
            try:
                uuid.UUID(str(val))
                return True
            except ValueError:
                return False
        
        df = df[df['date_uuid'].apply(is_valid_uuid)]
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df

    def clean_orders_data(self):
        """Cleans the orders data.

        Performs the following:
        1. Drops 'first_name', 'last_name' and '1' columns

        Returns:
            DataFrame: Cleaned DataFrame containing processed orders data.
        """
        df = self.data.copy()

        columns_to_remove = ['first_name', 'last_name', '1']
        df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df
        
if __name__ == "__main__":
    creds_file = r'c:/Users/safi-/OneDrive/Occupation/AiCore/AiCore Training/PROJECTS/' \
                                  'Multinational Retail Data Centralisation Project/multinational-retail-data-centralisation855/db_creds.yaml'
    db_connector = database_utils.DatabaseConnector(creds_file)

    data_extractor = DataExtractor(db_connector)

    # """Extract and clean user data."""
    # user_data = data_extractor.read_rds_table('legacy_users')
    # cleaner = DataCleaning(user_data)
    # cleaned_user_data = cleaner.clean_user_data()

    # """Extract and clean card data."""
    # pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 

    # card_data = data_extractor.retrieve_pdf_data(pdf_link)
    # cleaner = DataCleaning(card_data)
    # cleaned_card_data = cleaner.clean_card_data()

    """Extract and clean store data."""
    endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    
    number_of_stores = data_extractor.list_number_of_stores(endpoint, headers)  
    stores_details = []

    for store_number in range(0, 452):
        stores_data = data_extractor.retrieve_store_details(str(store_number), stores_endpoint, headers)
        stores_details.append(stores_data)

    stores_dataframe = pd.concat(stores_details, ignore_index=True)
    cleaner = DataCleaning(stores_dataframe)
    cleaned_store_data = cleaner.clean_store_data()

    # """Extract and clean product data."""
    # s3_address = "s3://data-handling-public/products.csv"
    # products_df = data_extractor.extract_from_s3(s3_address)

    # product_data = data_extractor.extract_from_s3(s3_address)
    # cleaner = DataCleaning(product_data)
    # cleaned_product_data = cleaner.clean_products_data(products_df)
    # cleaned_product_data = cleaner.convert_product_weights(cleaned_product_data)

    # """Extract and clean date time data."""
    # s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    # date_time_data = data_extractor.extract_from_s3(s3_address)
    # cleaner = DataCleaning(date_time_data)
    # cleaned_date_time_data = cleaner.clean_date_times_data()

    # """Extract and clean orders data."""
    # AWS_RDS_db_connector = database_utils.DatabaseConnector(creds_file)
    # AWS_RDS_db_connector.init_db_engine(db_type='source')
    # data_extractor = DataExtractor(db_connector)

    # orders_data = data_extractor.read_rds_table("orders_table")
    # cleaner = DataCleaning(orders_data)
    # cleaned_orders_data = cleaner.clean_orders_data()

    """Upload all tables to the local database."""
    local_db_connector = database_utils.DatabaseConnector(creds_file)
    local_db_connector.init_db_engine(db_type='local')

    # upload_table = "dim_users"
    # local_db_connector.upload_to_db(cleaned_user_data, upload_table, db_type='local')

    # card_upload_table = "dim_card_details"
    # local_db_connector.upload_to_db(cleaned_card_data, card_upload_table, db_type='local')

    store_upload_table = "dim_store_details"
    local_db_connector.upload_to_db(cleaned_store_data, store_upload_table, db_type='local')

    # products_upload_table = "dim_products"
    # local_db_connector.upload_to_db(cleaned_product_data, products_upload_table, db_type='local')

    # date_times_upload_table = "dim_date_times"
    # local_db_connector.upload_to_db(cleaned_date_time_data, date_times_upload_table, db_type='local')

    # orders_upload_table = "orders_table"
    # local_db_connector.upload_to_db(cleaned_orders_data, orders_upload_table, db_type='local')

    print("Tables in the local database:")
    local_db_connector.list_db_tables()
    