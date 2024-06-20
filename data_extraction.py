from sqlalchemy import select, MetaData, Table
import pandas as pd
import database_utils
import tabula
import tempfile
import os
import requests
import boto3
from urllib.parse import urlparse


class DataExtractor:
    """A class to extract data from a relational database.

    Attributes:
        db_connector (DatabaseConnector): An instance of DatabaseConnector.
    """

    def __init__(self, db_connector=None):
        """Initialises the DataExtractor instance with a database connector.

        Args:
            db_connector (DatabaseConnector): An instance of DatabaseConnector.
        """
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """Reads data from a specified table in the database and returns it as a DataFrame.

        Args:
            table_name (str): Name of the table.

        Returns:
            DataFrame: DataFrame containing the data from the specified table.
        """
        engine = self.db_connector.init_db_engine()
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        query = select(table.columns)

        with engine.connect() as connection:
            result_proxy = connection.execute(query)
            df = pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())

        return df
    
    def retrieve_pdf_data(self, pdf_link):
        """Extracts tables from a PDF document and returns a DataFrame.

        Args:
            pdf_link (str): URL to the PDF file.

        Returns:
            DataFrame: DataFrame containing the extracted data from the PDF.
        """
        if pdf_link.startswith('http'):
            response = requests.get(pdf_link)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tf:
                tf.write(response.content)
                temp_pdf_path = tf.name
        else:
            temp_pdf_path = pdf_link

        dfs = tabula.read_pdf(temp_pdf_path, pages='all', multiple_tables=True)

        if pdf_link.startswith('http'):
            os.remove(temp_pdf_path)

        df = pd.concat(dfs, ignore_index=True)

        return df
    
    def list_number_of_stores(self, endpoint, headers):
        """Retrieves the number of stores from the API.

        Args:
            endpoint (str): Endpoint URL to retrieve the number of stores.
            headers (dict): Dictionary containing headers for the API request.

        Returns:
            int: Number of stores.
        """
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                return int(data['number_stores'])
            except KeyError:
                print(f"Unexpected JSON structure: {data}")
                return None
        else:
            print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
            return None

    def retrieve_stores_data(self, endpoint, headers):
        """Retrieves store from the API endpoint and saves into a pandas DataFrame.

        Args:
            endpoint (str): Endpoint URL to retrieve the number of stores.
            headers (dict): Dictionary containing headers for the API request.

        Returns:
            DataFrame: DataFrame containing the stores metadata.
        """
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            data = response.json()
                
            if 'number_stores' in data:
                df = pd.DataFrame([data])
                return df
            else:
                print("No 'number_stores' key found in JSON data.")
                return None
        else:
            print(f"Failed to retrieve stores metadata. Status code: {response.status_code}")
            return None
    
    def extract_from_s3(self, s3_address):
        """Extracts data from S3 bucket and returns it as a DataFrame.

        Args:
            s3_address (str): S3 address to the csv file.

        Returns:
            DataFrame: DataFrame containing the data from the S3 address.
        """
        s3 = boto3.client('s3')

        parsed_url = urlparse(s3_address)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tf:
            temp_csv_path = tf.name

        s3.download_file(bucket_name, key, temp_csv_path)
        df = pd.read_csv(temp_csv_path)
        os.remove(temp_csv_path)

        return df