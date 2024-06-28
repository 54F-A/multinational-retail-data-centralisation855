-- Change the data types to correspond to those seen in the table below:
-- +------------------+--------------------+--------------------+
-- |   orders_table   | current data type  | required data type |
-- +------------------+--------------------+--------------------+
-- | date_uuid        | TEXT               | UUID               |
-- | user_uuid        | TEXT               | UUID               |
-- | card_number      | TEXT               | VARCHAR(?)         |
-- | store_code       | TEXT               | VARCHAR(?)         |
-- | product_code     | TEXT               | VARCHAR(?)         |
-- | product_quantity | BIGINT             | SMALLINT           |
-- +------------------+--------------------+--------------------+
-- The ? in VARCHAR should be replaced with an integer representing the maximum length of the values in that column.

--To find ? in VARCHAR:
SELECT MAX(LENGTH(CAST(column_name AS TEXT))) FROM table_name;

--To delete row:
DELETE FROM table_name WHERE column_name = '########';

ALTER TABLE orders_table
    ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(19),
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(11),
    ALTER COLUMN product_quantity SET DATA TYPE SMALLINT;

-- The column required to be changed in the users table are as follows:
-- +----------------+--------------------+--------------------+
-- | dim_users      | current data type  | required data type |
-- +----------------+--------------------+--------------------+
-- | first_name     | TEXT               | VARCHAR(255)       |
-- | last_name      | TEXT               | VARCHAR(255)       |
-- | date_of_birth  | TEXT               | DATE               |
-- | country_code   | TEXT               | VARCHAR(?)         |
-- | user_uuid      | TEXT               | UUID               |
-- | join_date      | TEXT               | DATE               |
-- +----------------+--------------------+--------------------+

ALTER TABLE dim_users
    ALTER COLUMN first_name SET DATA TYPE VARCHAR(225),
    ALTER COLUMN last_name SET DATA TYPE VARCHAR(225),
    ALTER COLUMN date_of_birth SET DATA TYPE DATE,
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(2),
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date SET DATA TYPE DATE;

-- Set the data types for each column as shown below:
-- +---------------------+-------------------+------------------------+
-- | store_details_table | current data type |   required data type   |
-- +---------------------+-------------------+------------------------+
-- | longitude           | TEXT              | FLOAT                  |
-- | locality            | TEXT              | VARCHAR(255)           |
-- | store_code          | TEXT              | VARCHAR(?)             |
-- | staff_numbers       | TEXT              | SMALLINT               |
-- | opening_date        | TEXT              | DATE                   |
-- | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
-- | latitude            | TEXT              | FLOAT                  |
-- | country_code        | TEXT              | VARCHAR(?)             |
-- | continent           | TEXT              | VARCHAR(255)           |
-- +---------------------+-------------------+------------------------+

ALTER TABLE dim_store_details
    ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::double precision,
    ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::double precision,
    ALTER COLUMN locality SET DATA TYPE VARCHAR(225),
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(11),
    ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::smallint,
    ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::date,
    ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(2),
    ALTER COLUMN continent SET DATA TYPE VARCHAR(255);
ALTER TABLE dim_store_details
	ALTER COLUMN store_type DROP NOT NULL;

-- Reomve £ character from the product_price column.
-- Add a new column weight_class which will contain human-readable values based on the weight range of the product.
-- +--------------------------+-------------------+
-- | weight_class VARCHAR(?)  | weight range(kg)  |
-- +--------------------------+-------------------+
-- | Light                    | < 2               |
-- | Mid_Sized                | >= 2 - < 40       |
-- | Heavy                    | >= 40 - < 140     |
-- | Truck_Required           | => 140            |
-- +----------------------------+-----------------+

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) < 2 THEN 'Light'
    WHEN CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) >= 2 AND CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) < 40 THEN 'Mid_Sized'
    WHEN CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) >= 40 AND CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) < 140 THEN 'Heavy'
    WHEN CAST(REPLACE(weight, ' kg', '') AS DECIMAL(10, 2)) >= 140 THEN 'Truck_Required'
END;

-- Make the changes to the columns to cast them to the following data types:

-- +-----------------+--------------------+--------------------+
-- |  dim_products   | current data type  | required data type |
-- +-----------------+--------------------+--------------------+
-- | product_price   | TEXT               | FLOAT              |
-- | weight          | TEXT               | FLOAT              |
-- | EAN             | TEXT               | VARCHAR(?)         |
-- | product_code    | TEXT               | VARCHAR(?)         |
-- | date_added      | TEXT               | DATE               |
-- | uuid            | TEXT               | UUID               |
-- | still_available | TEXT               | BOOL               |
-- | weight_class    | TEXT               | VARCHAR(?)         |
-- +-----------------+--------------------+--------------------+

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');

UPDATE dim_products
SET still_available = CASE
    WHEN still_available ILIKE 'Still_avaliable' THEN 'true'
    WHEN still_available ILIKE 'Removed' THEN 'false'
END;

ALTER TABLE dim_products
    ALTER COLUMN product_price SET DATA TYPE FLOAT USING product_price::double precision,
    ALTER COLUMN weight SET DATA TYPE FLOAT USING weight::double precision,
    ALTER COLUMN EAN SET DATA TYPE VARCHAR()
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(11),
    ALTER COLUMN date_added SET DATA TYPE DATE USING date_added::date,
    ALTER COLUMN uuid SET DATA TYPE UUID USING uuid::uuid,
    ALTER COLUMN still_available SET DATA TYPE BOOL USING still_available::bool,
    ALTER COLUMN weight_class SET DATA TYPE VARCHAR(14);

SELECT * FROM dim_products