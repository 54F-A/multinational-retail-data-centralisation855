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
        1. Maps country to country_code.
        2. Converts date_of_birth values to the foramt: 'YYYY-MM-DD'
        3. Converts join_date values to the foramt: 'YYYY-MM-DD'
        4. Removes whitespace, dots, and hyphens from the phone_number column.

        Returns:
            DataFrame: Cleaned DataFrame containing processed user data.
        """
        df = self.data.copy()

        country_mapping = {
            'Germany': 'DE',
            'United Kingdom': 'GB',
            'United States': 'US'
        }
        df['country_code'] = df['country'].map(country_mapping)
        df = df[df['country_code'].notna()]

        df['date_of_birth'] = df['date_of_birth'].apply(lambda dob: 
            pd.to_datetime(dob) if pd.notnull(dob) else dob)
        df = df.dropna(subset=['date_of_birth'])
        df['date_of_birth'] = df['date_of_birth'].apply(lambda dob: 
            dob.strftime('%Y-%m-%d') if isinstance(dob, pd.Timestamp) else dob)

        df['join_date'] = df['join_date'].apply(lambda jd: 
            pd.to_datetime(jd) if pd.notnull(jd) else jd)
        df = df.dropna(subset=['join_date'])
        df['join_date'] = df['join_date'].apply(lambda jd: 
            jd.strftime('%Y-%m-%d') if isinstance(jd, pd.Timestamp) else jd)

        df['phone_number'] = df['phone_number'].apply(lambda number:
            re.sub(r'\s+|\.|-', '', number) if pd.notnull(number) else number)
        df = df[df['phone_number'].notna()]

        df.reset_index(drop=True, inplace=True)
        self.data = df
        
        return df
    
    def clean_card_data(self):
        """Cleans the card data.

        Performs the following:
        1. Removes expiry_date values that are not in the format: 'MM/YY'
        2. Removes '?' in the card_number column.
        3. Converts date_payment_confirmed values to the foramt: 'YYYY-MM-DD'

        Returns:
            DataFrame: Cleaned DataFrame containing processed card data.
        """
        df = self.data.copy()
        
        valid_expiry_date = re.compile(r'^(0[1-9]|1[0-2])\/\d{2}$')
        df = df[df['expiry_date'].apply(lambda x: bool(valid_expiry_date.match(x)))]

        df['card_number'] = df['card_number'].astype(str)
        df['card_number'] = df['card_number'].str.replace('?', '')

        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda dpc: 
            pd.to_datetime(dpc) if pd.notnull(dpc) else dpc)
        df = df.dropna(subset=['date_payment_confirmed'])
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda dpc: 
            dpc.strftime('%Y-%m-%d') if isinstance(dpc, pd.Timestamp) else dpc)

        df.reset_index(drop=True, inplace=True)
        self.data = df

        return df

    def clean_store_data(self):
        """Cleans the store data.

        Performs the following:
        1. Removes values from the longitude column that are neither floats nor 'N/A'.
        2. Removes 'lat' column.
        3. Moves latitude column to the third position in the table.
        4. Removes staff_numbers values that are non-numerical.

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
    
    @staticmethod
    def convert_weight(weight):
            """Converts product weight to kg.

            Args:
                weight (str): Weight of the product as a string.

            Returns:
                str: Weight converted to kg as a string.
            """
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

    def convert_product_weights(self, products_df):
        """Converts product weight to kg.

        Performs the following:
        1. Converts weight values from g to kg.

        Args:
            products_df (DataFrame): DataFrame containing product data.

        Returns:
            DataFrame: Cleaned DataFrame with weights converted to kg.
        """
        df = products_df.copy()

        convert_weight = DataCleaning.convert_weight
        products_df['weight'] = products_df['weight'].apply(convert_weight)

        products_df.reset_index(drop=True, inplace=True)
        self.data = df

        return products_df
    
    def clean_products_data(self, products_df):
        """Cleans the product data.

        Performs the following:
        1. Drops missing values.
        2. Filters rows where removed column is either 'Still_avaliable' or 'Removed'.

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
    
    @staticmethod
    def is_valid_uuid(value):
            """Checks if the value is a valid UUID.

            Args:
                value (str): The value to be checked.

            Returns:
                bool: True if the value is a valid UUID.
            """
            try:
                uuid.UUID(str(value))
                return True
            except ValueError:
                return False

    def clean_date_times_data(self):
        """Cleans the date details data.

        Performs the following:
        1. Drops rows with missing values.
        2. Filters rows with valid UUIDs in the date_uuid column.

        Returns:
            DataFrame: Cleaned DataFrame containing processed date details data.
        """
        df = self.data.copy()

        df.dropna(inplace=True) 
        is_valid_uuid = DataCleaning.is_valid_uuid
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
    