from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from pathlib import Path
import database_utils
import pandas as pd
import yaml


def config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

config_file = Path('db_creds.yaml')
config = config(config_file)

creds_file = config['creds_file']
pdf_link = config['pdf_link']
endpoint = config['endpoint']
stores_endpoint = config['stores_endpoint']
headers = config['headers']
s3_address_products = config['s3_address_products']
s3_address_dates = config['s3_address_dates']

if __name__ == '__main__':
    db_connector_1 = database_utils.DatabaseConnector(creds_file)
    db_connector_1.init_db_engine(db_type='local')

    db_connector_2 = database_utils.DatabaseConnector(creds_file)
    db_connector_2.init_db_engine(db_type='source')

    data_extractor = DataExtractor(db_connector_2)

    def orders_table_run():
        '''Cleans the extracted orders data & uploads cleaned data to local database.'''
        orders_data = data_extractor.read_rds_table("orders_table")
        cleaner = DataCleaning(orders_data)
        cleaned_orders_data = cleaner.clean_orders_data()
        db_connector_1.upload_to_db(cleaned_orders_data, 'orders_table', db_type='local')
    orders_table_run()

    def dim_users_run():
        '''Cleans the extracted users data & uploads cleaned data to local database.'''
        user_data = data_extractor.read_rds_table('legacy_users')
        cleaner = DataCleaning(user_data)
        cleaned_user_data = cleaner.clean_user_data()
        db_connector_1.upload_to_db(cleaned_user_data, 'dim_users', db_type='local')
    dim_users_run()

    def dim_cards_run():
        '''Cleans the extracted cards data & uploads cleaned data to local database.'''
        card_data = data_extractor.retrieve_pdf_data(pdf_link)
        cleaner = DataCleaning(card_data)
        cleaned_card_data = cleaner.clean_card_data()
        db_connector_1.upload_to_db(cleaned_card_data, 'dim_card_details', db_type='local')
    dim_cards_run()

    def dim_stores_run():      
        '''Cleans the extracted stores data & uploads cleaned data to local database.'''  
        number_of_stores = data_extractor.list_number_of_stores(endpoint, headers)  
        stores_details = []

        for store_number in range(0, 452):
            stores_data = data_extractor.retrieve_store_details(str(store_number), stores_endpoint, headers)
            stores_details.append(stores_data)

        stores_dataframe = pd.concat(stores_details, ignore_index=True)
        cleaner = DataCleaning(stores_dataframe)
        cleaned_store_data = cleaner.clean_store_data()
        db_connector_1.upload_to_db(cleaned_store_data, 'dim_store_details', db_type='local')
    dim_stores_run()

    def dim_products_run():
        '''Cleans the extracted products data & uploads cleaned data to local database.'''
        products_df = data_extractor.extract_from_s3(s3_address_products)
        product_data = data_extractor.extract_from_s3(s3_address_products)
        cleaner = DataCleaning(product_data)
        cleaned_product_data = cleaner.clean_products_data(products_df)
        cleaned_product_data = cleaner.convert_product_weights(cleaned_product_data)
        db_connector_1.upload_to_db(cleaned_product_data, 'dim_products', db_type='local')
    dim_products_run()

    def dim_dates_run():
        '''Cleans the extracted dates data & uploads cleaned data to local database.'''
        date_time_data = data_extractor.extract_from_s3(s3_address_dates)
        cleaner = DataCleaning(date_time_data)
        cleaned_date_time_data = cleaner.clean_date_times_data()
        db_connector_1.upload_to_db(cleaned_date_time_data, 'dim_date_times', db_type='local')
    dim_dates_run()