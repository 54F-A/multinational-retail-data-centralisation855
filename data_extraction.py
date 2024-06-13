import csv
import requests
import boto3
from io import StringIO


class DataExtractor:
    def __init__(self):
        pass

    def extract_from_csv(self, file_path):
        """
        Extract data from a CSV file.

        :param file_path: str, path to the CSV file
        :return: list of dicts, each dict represents a row in the CSV file
        """
        data = []
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data.append(row)
        return data

    def extract_from_api(self, api_url, headers=None, params=None):
        """
        Extract data from an API.

        :param api_url: str, the URL of the API endpoint
        :param headers: dict, (optional) headers to include in the request
        :param params: dict, (optional) parameters to include in the request
        :return: dict, the JSON response from the API
        """
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()

    def extract_from_s3(self, bucket_name, file_key, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Extract data from an S3 bucket.

        :param bucket_name: str, the name of the S3 bucket
        :param file_key: str, the key (path) of the file in the S3 bucket
        :param aws_access_key_id: str, (optional) AWS access key ID
        :param aws_secret_access_key: str, (optional) AWS secret access key
        :return: str, the contents of the file
        """
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return obj['Body'].read().decode('utf-8')

# Example usage:
if __name__ == "__main__":
    extractor = DataExtractor()
    
    # Extract data from a CSV file
    csv_data = extractor.extract_from_csv('path/to/your/file.csv')
    print(csv_data)
    
    # Extract data from an API
    api_data = extractor.extract_from_api('https://api.example.com/data')
    print(api_data)
    
    # Extract data from an S3 bucket
    s3_data = extractor.extract_from_s3('your-bucket-name', 'path/to/your/file.csv')
    print(s3_data)