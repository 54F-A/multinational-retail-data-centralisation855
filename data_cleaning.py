import pandas as pd


class DataCleaning:
    def __init__(self):
        pass

    def clean_csv_data(self, data):
        """
        Clean data extracted from a CSV file.

        :param data: list of dicts, the data extracted from the CSV file
        :return: DataFrame, the cleaned data as a pandas DataFrame
        """
        df = pd.DataFrame(data)
        
        # Example cleaning steps:
        df.dropna(inplace=True)  # Drop rows with missing values
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]  # Normalize column names

        return df

    def clean_api_data(self, data):
        """
        Clean data extracted from an API.

        :param data: dict, the data extracted from the API
        :return: dict, the cleaned data
        """
        # Example cleaning steps:
        # Convert all keys to lowercase
        cleaned_data = {k.lower(): v for k, v in data.items()}
        
        # Additional cleaning can be added here as required

        return cleaned_data

    def clean_s3_data(self, data):
        """
        Clean data extracted from an S3 bucket.

        :param data: str, the data extracted from the S3 bucket (assumed to be CSV content)
        :return: DataFrame, the cleaned data as a pandas DataFrame
        """
        df = pd.read_csv(pd.compat.StringIO(data))
        
        # Example cleaning steps:
        df.dropna(inplace=True)  # Drop rows with missing values
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]  # Normalize column names

        return df

# Example usage:
if __name__ == "__main__":
    cleaner = DataCleaning()

    # Example CSV data
    csv_data = [
        {'Name': 'Alice', ' Age': 25, ' Salary ': 70000},
        {'Name': 'Bob', ' Age': None, ' Salary ': 80000},
        {'Name': 'Charlie', ' Age': 30, ' Salary ': None}
    ]
    cleaned_csv_data = cleaner.clean_csv_data(csv_data)
    print("Cleaned CSV Data:")
    print(cleaned_csv_data)