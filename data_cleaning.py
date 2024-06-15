import pandas as pd
from data_extraction import DataExtractor
import database_utils
import re


class DataCleaning:
    def __init__(self, data):
        self.data = data
    
    def clean_user_data(self):
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
            if country_code == 'GB':
                if any(char not in '0123456789()+ ' for char in phone_number) or (len(re.sub(r'\D', '', phone_number)) > 13):
                    return None
                elif phone_number.startswith('0') or phone_number.startswith('(') or phone_number.startswith('+'):
                    return phone_number
                else:
                    phone_number
            elif country_code == 'US': 
                if len(re.sub(r'\D', '', phone_number)) > 12 or any(char not in '0123456789()+ ' for char in phone_number):
                    return None
                elif phone_number.startswith('0') or phone_number.startswith('(') or phone_number.startswith('+'):
                    return phone_number
                else:
                    return phone_number
            elif country_code == 'DE': 
                if len(re.sub(r'\D', '', phone_number)) > 12 or any(char not in '0123456789()+ ' for char in phone_number):
                    return None
                elif phone_number.startswith('0') or phone_number.startswith('(') or phone_number.startswith('+'):
                    return phone_number
                else:
                    return phone_number
            else:
                return None

        df['phone_number'] = df.apply(lambda row: format_phone_number(row['phone_number'], row['country_code']), axis=1)
        
        df.reset_index(drop=True, inplace=True)
        self.data = df
    
    def get_clean_data(self):
        return self.data

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

    upload_table = "dim_users"
    db_connector.upload_to_db(cleaned_df, upload_table)

    print(f"Cleaned data successfully uploaded to table: '{upload_table}'")
