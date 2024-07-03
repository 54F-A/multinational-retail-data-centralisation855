from sqlalchemy import select, MetaData, Table
from urllib.parse import urlparse
import boto3
import os
import pandas as pd
import requests
import tabula
import tempfile


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

        temp_pdf_path = None
        
        if pdf_link.startswith('http'):
            response = requests.get(pdf_link)
            response.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tf:
                tf.write(response.content)
                temp_pdf_path = tf.name
        else:
            temp_pdf_path = pdf_link

        dfs = tabula.read_pdf(temp_pdf_path, pages='all', multiple_tables=True)
        df = pd.concat(dfs, ignore_index=True)

        if temp_pdf_path and pdf_link.startswith('http'):
            os.remove(temp_pdf_path)

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
            data = response.json()
            df = pd.DataFrame([data])

        return df
   
    def retrieve_store_details(self, store_number, endpoint, headers):
        """Extracts data from the API and returns it as a DataFrame.

        Args:
            store_number: The unique identifier for the store
            endpoint (str): Endpoint URL to retrieve the number of stores.
            headers (dict): Dictionary containing headers for the API request.

        Returns:
            DataFrame: A DataFrame containing the store data.
        """
        endpoint = endpoint.format(store_number=store_number)
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            store_data = response.json()
            df = pd.DataFrame([store_data])
            
            return df

    def extract_from_s3(self, s3_address):
        """Extracts data from S3 bucket and returns it as a DataFrame.

        Args:
            s3_address (str): S3 address to the csv file.

        Returns:
            DataFrame: DataFrame containing the data from the S3 address.
        """
        s3 = boto3.client('s3')

        parsed_url = urlparse(s3_address)
        bucket_name = parsed_url.netloc.split('.')[0]
        key = parsed_url.path.lstrip('/')

        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(key)[1]) as tf:
            temp_file_path = tf.name

        s3.download_file(bucket_name, key, temp_file_path)
        file_extension = os.path.splitext(temp_file_path)[1]
        
        if file_extension == '.csv':
            df = pd.read_csv(temp_file_path)
        elif file_extension == '.json':
            df = pd.read_json(temp_file_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        
        os.remove(temp_file_path)
        
        return df