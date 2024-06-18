import pandas as pd
from data_extraction import DataExtractor
import database_utils
import re


class DataCleaning:
    """A class to clean and preprocess user data extracted from a database.

    Attributes:
        data (DataFrame): The DataFrame containing user data extracted from the database.
    """

    def __init__(self, data):
        """Initializes the DataCleaning instance with the provided DataFrame.

        Args:
            data (DataFrame): The initial DataFrame containing user data extracted from the database.
        """
        self.data = data
    
    def clean_user_data(self):
        """Cleans and preprocesses the user data.

        Performs the following:
        1. Drops rows with missing values.
        2. Converts 'date_of_birth' and 'join_date' columns to datetime format.
        3. Maps 'country' names to 'country_code' using a predefined mapping.
        4. Formats 'phone_number' based on 'country_code' using regex patterns.
        5. Updates the 'data' attribute with the cleaned DataFrame.
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
    
    def get_clean_data(self):
        """Returns the cleaned DataFrame.

        Returns:
            DataFrame: The cleaned DataFrame containing processed user data.
        """
        return self.data
    
    def clean_card_data(self):
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
            if provider in valid_lengths:
                return len(card_number) in valid_lengths[provider]
            return False

        df = df[df.apply(lambda row: is_valid_length(row['card_number'], row['card_provider']), axis=1)]

        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        current_date = pd.Timestamp.now()
        df = df[df['expiry_date'] >= current_date]

        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(subset=['date_payment_confirmed'], inplace=True)

        df.reset_index(drop=True, inplace=True)
        self.data = df

if __name__ == "__main__":
    creds_file = r'c:/Users/safi-/OneDrive/Occupation/AiCore/AiCore Training/PROJECTS/' \
                                  'Multinational Retail Data Centralisation Project/multinational-retail-data-centralisation855/db_creds.yaml'
    db_connector = database_utils.DatabaseConnector(creds_file)

    data_extractor = DataExtractor(db_connector)
    table_name = "legacy_users"
    df = data_extractor.read_rds_table(table_name)

    print(f"\nOriginal DataFrame for table '{table_name}':")
    print(df)

    data_cleaner = DataCleaning(df)
    data_cleaner.clean_user_data()
    cleaned_df = data_cleaner.get_clean_data()

    print(f"\nCleaned DataFrame for table '{table_name}':")
    print(cleaned_df)

    local_db_connector = database_utils.DatabaseConnector(creds_file)
    local_db_connector.init_db_engine(db_type='local')
    upload_table = "dim_users"
    local_db_connector.upload_to_db(cleaned_df, upload_table, db_type='local')

    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 
    card_df = data_cleaner.retrieve_pdf_data(pdf_link)
    data_cleaner.data = card_df
    data_cleaner.clean_card_data()
    cleaned_card_df = data_cleaner.get_clean_data()

    print(f"\nCleaned DataFrame from PDF:")
    print(cleaned_card_df)