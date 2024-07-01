from data_extraction import DataExtractor
import database_utils
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
        1. Drops rows with missing values.
        2. Converts 'date_of_birth' and 'join_date' columns to datetime format.
        3. Maps 'country' names to 'country_code' using a predefined mapping.
        4. Formats 'phone_number' based on 'country_code' using regex patterns.
        5. Updates the 'data' attribute with the cleaned DataFrame.

        Returns:
            DataFrame: Cleaned DataFrame containing processed user data.
        """
        df = self.data.copy()

        df.dropna(inplace=True)
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
            """Formats the phone number based on the country code using regex patterns.

            Args:
                phone_number (str): The original phone number.
                country_code (str): The country code corresponding to the phone number.

            Returns:
                str or None: The formatted phone number if valid, otherwise None.
            """
            clean_number = re.sub(r'[^0-9+(]', '', phone_number)
            gb_pattern = re.compile(r'^\+?44\s?\(?0?\)?(\d{2})\D?(\d{4})\D?(\d{0,4})$')
            us_pattern = re.compile(r'^\+?1\s?\(?(\d{3})\)?\D?(\d{3})\D?(\d{4})$')
            de_pattern = re.compile(r'^\+?49\s?\(?0?(\d{2})\D?(\d{3})\D?(\d{4})$')

            if country_code == 'GB':
                match = gb_pattern.match(clean_number)
                if match and len(''.join(match.groups())) == 10:
                    return f"+44 {match.group(1)} {match.group(2)} {match.group(3)}".strip()
                else:
                    return None
            elif country_code == 'US':
                match = us_pattern.match(clean_number)
                if match and len(''.join(match.groups())) == 10:
                    return f"+1 {match.group(1)}-{match.group(2)}-{match.group(3)}"
                else:
                    return None
            elif country_code == 'DE':
                match = de_pattern.match(clean_number)
                if match and len(''.join(match.groups())) == 10:
                    return f"+49 {match.group(1)} {match.group(2)} {match.group(3)}"
                else:
                    return None
            else:
                return None

        df['phone_number'] = df.apply(lambda row: format_phone_number(row['phone_number'], row['country_code']), axis=1)
        df = df.dropna(subset=['phone_number'])
        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df
    
    def clean_card_data(self):
        """Cleans the card data.

        Performs the following:
        1. Drops rows with missing values.
        2. Strips non-numeric characters from 'card_number'.
        3. Validates 'card_number' lengths based on 'card_provider'.
        4. Converts 'expiry_date' to datetime and filters out expired cards.
        5. Converts 'date_payment_confirmed' to datetime and drops rows with invalid dates.

        Returns:
            DataFrame: Cleaned DataFrame containing processed card data.
        """
        df = self.data.copy()

        df.dropna(inplace=True)
        df['card_number'] = df['card_number'].apply(lambda x: re.sub(r'\D', '', str(x)))

        valid_lengths = {
            'Diners Club / Carte Blanche': [14],
            'American Express': [15],
            'JCB 16 digit': [16],
            'JCB 15 digit': [15],
            'Maestro': [12, 13, 19],
            'Mastercard': [16],
            'Discover': [16],
            'VISA 16 digit': [16],
            'VISA 13 digit': [13],
            'VISA 19 digit': [19]
        }

        def is_valid_length(card_number, provider):
            """Checks if the card number length is valid.

            Args:
                card_number (str): The card number to be validated.
                provider (str): The card provider corresponding to the card number.

            Returns:
                bool: True if the card number length is valid for the given provider, otherwise False.
            """
            return len(card_number) in valid_lengths.get(provider, [])

        df = df[df.apply(lambda row: is_valid_length(row['card_number'], row['card_provider']), axis=1)]

        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        current_date = pd.Timestamp.now()
        df = df[df['expiry_date'] >= current_date]

        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(subset=['date_payment_confirmed'], inplace=True)

        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df

    def clean_store_data(self):
        """Cleans the store data.

        Performs the following:
        1. Removes the 'lat' column.
        2. Moves the 'latitude' column to the 4th position.
        3. Drops row 63
        4. Drops rows where 'staff_numbers' column are non-numerical.
        5. Converts 'opening_date' column to 'YYYY-MM-DD' format.
        6. Drops missing values from the 'opening_date' column.

        Returns:
            DataFrame: Cleaned DataFrame containing processed store data.
        """
        df = self.data.copy()

        columns_to_remove = ['lat']
        df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
        df.insert(3, 'latitude', df.pop('latitude'))
        df.drop(index=63, inplace=True)
        df = df[pd.to_numeric(df['staff_numbers'], errors='coerce').notnull()]
        mask = df['opening_date'].str.contains(r'^\d{4}-\d{2}-\d{2}$', na=False)
        df.loc[~mask, 'opening_date'] = pd.to_datetime(df.loc[~mask, 'opening_date'], format='%B %Y %d', errors='coerce').dt.strftime('%Y-%m-%d')
        df.dropna(subset=['opening_date'], inplace=True)
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
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        df.reset_index(drop=True, inplace=True)

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

    """Extract and clean user data."""
    user_data = data_extractor.read_rds_table('legacy_users')
    cleaner = DataCleaning(user_data)
    cleaned_user_data = cleaner.clean_user_data()

    """Extract and clean card data."""
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 

    card_data = data_extractor.retrieve_pdf_data(pdf_link)
    cleaner = DataCleaning(card_data)
    cleaned_card_data = cleaner.clean_card_data()

    """Print number of stores."""
    endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    number_of_stores = data_extractor.list_number_of_stores(endpoint, headers)
    total_stores = number_of_stores.iloc[0, 0]

    """Extract and clean store data."""
    endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    
    stores_details = []

    for store_number in range(1, (total_stores + 1)):
        stores_data = data_extractor.retrieve_store_details(str(store_number), endpoint, headers)
        stores_details.append(stores_data)

    stores_dataframe = pd.concat(stores_details, ignore_index=True)
    cleaner = DataCleaning(stores_dataframe)
    cleaned_store_data = cleaner.clean_store_data()

    """Extract and clean product data."""
    s3_address = "s3://data-handling-public/products.csv"
    products_df = data_extractor.extract_from_s3(s3_address)

    product_data = data_extractor.extract_from_s3(s3_address)
    cleaner = DataCleaning(product_data)
    cleaned_product_data = cleaner.clean_products_data(products_df)
    cleaned_product_data = cleaner.convert_product_weights(cleaned_product_data)

    """Extract and clean date time data."""
    s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    date_time_data = data_extractor.extract_from_s3(s3_address)
    cleaner = DataCleaning(date_time_data)
    cleaned_date_time_data = cleaner.clean_date_times_data()

    """Extract and clean orders data."""
    AWS_RDS_db_connector = database_utils.DatabaseConnector(creds_file)
    AWS_RDS_db_connector.init_db_engine(db_type='source')
    data_extractor = DataExtractor(db_connector)

    orders_data = data_extractor.read_rds_table("orders_table")
    cleaner = DataCleaning(orders_data)
    cleaned_orders_data = cleaner.clean_orders_data()

    """Upload all tables to the local database."""
    local_db_connector = database_utils.DatabaseConnector(creds_file)
    local_db_connector.init_db_engine(db_type='local')

    upload_table = "dim_users"
    local_db_connector.upload_to_db(cleaned_user_data, upload_table, db_type='local')

    card_upload_table = "dim_card_details"
    local_db_connector.upload_to_db(cleaned_card_data, card_upload_table, db_type='local')

    store_upload_table = "dim_store_details"
    local_db_connector.upload_to_db(cleaned_store_data, store_upload_table, db_type='local')

    products_upload_table = "dim_products"
    local_db_connector.upload_to_db(cleaned_product_data, products_upload_table, db_type='local')

    date_times_upload_table = "dim_date_times"
    local_db_connector.upload_to_db(cleaned_date_time_data, date_times_upload_table, db_type='local')

    orders_upload_table = "orders_table"
    local_db_connector.upload_to_db(cleaned_orders_data, orders_upload_table, db_type='local')

    print("Tables in the local database:")
    local_db_connector.list_db_tables()
    