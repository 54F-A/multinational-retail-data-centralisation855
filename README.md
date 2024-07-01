# Multinational Retail Data Centralisation

## Table of Contents:

#### [1. Overview: The Project Description](#1-overview-the-project-description)
#### [2. Installation & Usage Instructions](#2-installation--usage-instructions)
#### [3. File Structure of the Project](#3-file-structure-of-the-project)
- #### [Data Extraction](#4-data-extraction)
- #### [Data Cleaning](#5-data-cleaning)
- #### [Data Uploading](#5-data-uploading)
- #### [Database Schema](#database-schema)
#### [4. License information](#6-license-information)

---

### Overview: The Project Description

This project focuses on centralising data from multiple sources into a unified database for a multinational retail company. The data is extracted from various sources including a relational database, PDFs, APIs, and S3 buckets, cleaned, and then uploaded to a local database.

---

### Installation & Usage Instructions

To run the project, you need to have the required database credentials. Ensure you have these in a YAML file.

Follow these steps:
1. Clone the repository to your local machine: __`git clone https://github.com/yourusername/multinational-retail-data-centralisation.git`__
2. Navigate to the project directory: __`cd multinational-retail-data-centralisation/`__
3. Install the required packages using: __`pip install`__
4. Run the cleaning file: __`python database_cleaning.py`__

---

### File Structure of the Project

- Extracts data from various sources (__databases, PDFs, APIs, S3__).
- Cleans and preprocesses the data.
- Uploads the cleaned data to a local database.
- Creates a database schema using primary keys and foreign keys.

---

### Data Extraction

#### Extracting User Data:

Data is extracted from the legacy_users table in the database.

__`table_name = "legacy_users"`__

__`legacy_users_df = data_extractor.read_rds_table(table_name)`__

#### Extracting Card Data:

Data is extracted from a PDF file containing card details.

__`pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"`__

__`card_df = data_extractor.retrieve_pdf_data(pdf_link)`__

#### Extracting Stores Data:

Store data is retrieved from an API endpoint.

__`api_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"`__

__`api_headers = {'x-api-key': 'your_api_key_here'}`__

__`stores_df = data_extractor.retrieve_stores_data(api_endpoint, api_headers)`__

#### Extracting Products Data:

Product data is extracted from a CSV file stored in an S3 bucket.

__`s3_address = "s3://data-handling-public/products.csv"`__

__`products_df = data_extractor.extract_from_s3(s3_address)`__

#### Extracting Date Details Data:

Date details are extracted from a JSON file stored in an S3 bucket.

__`s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"`__

__`date_times_df = data_extractor.extract_from_s3(s3_address)`__

---

### Data Cleaning

#### Cleaning User Data:

The user data is cleaned.

__`data_cleaner = DataCleaning(legacy_users_df)`__

__`cleaned_users_df = data_cleaner.clean_user_data()`__

#### Cleaning Card Data:

The card data is cleaned and invalid entries are removed.

__`data_cleaner = DataCleaning(card_df)`__

__`cleaned_card_df = data_cleaner.clean_card_data()`__

#### Cleaning Store Data:

The store data is cleaned to remove any invalid entries.

__`data_cleaner = DataCleaning(stores_df)`__

__`cleaned_store_df = data_cleaner.clean_store_data()`__

#### Cleaning Products Data:

Product weights are converted to kg and the data is cleaned.

__`data_cleaner = DataCleaning(products_df)`__

__`products_df = data_cleaner.convert_product_weights(products_df)`__

__`cleaned_products_df = data_cleaner.clean_products_data(products_df)`__

#### Cleaning Date Details Data:

The date details data is cleaned and invalid entries are removed.

__`data_cleaner = DataCleaning(date_times_df)`__

__`cleaned_date_times_df = data_cleaner.clean_date_times_data()`__

#### Cleaning Orders Data:

The orders data is cleaned and unnecessary columns are dropped.

__`data_cleaner = DataCleaning(orders_df)`__
__`cleaned_orders_df = data_cleaner.clean_orders_data()`__

---

### Data Uploading:

Uploads the cleaned data to the local database.

__`local_db_connector.upload_to_db(cleaned_users_df, "dim_users", db_type='local')`__

__`local_db_connector.upload_to_db(cleaned_card_df, "dim_card_details", db_type='local')`__

__`local_db_connector.upload_to_db(cleaned_store_df, "dim_store_details", db_type='local')`__

__`local_db_connector.upload_to_db(cleaned_products_df, "dim_products", db_type='local')`__

__`local_db_connector.upload_to_db(cleaned_date_times_df, "dim_date_times", db_type='local')`__

__`local_db_connector.upload_to_db(cleaned_orders_df, "orders_table", db_type='local')`__

---

### Database Schema:

Alters the data in the local database using the query tool.

Finds the maximum length of the values in a given column.

__`SELECT MAX(LENGTH(CAST(column_name AS TEXT))) FROM table_name;`__

Alter the data type of a given column.

__`ALTER COLUMN column_name SET DATA TYPE data_type;`__

Create a primary key.

__`ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;`__

__`ALTER TABLE table_name ADD UNIQUE (column_name);`__

__`ALTER TABLE table_name ADD PRIMARY KEY (column_name);`__

Create a foreign key.

__`ALTER TABLE table_name`__

__`ADD FOREIGN KEY (column_name)`__

__`REFERENCES dim_users(column_name);`__

---

### License information

This project is licensed under the MIT License.