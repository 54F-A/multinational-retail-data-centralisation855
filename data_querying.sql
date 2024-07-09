-- Which countries we currently operate in and which country has the most stores.

SELECT
    s.country_code AS country,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details s
GROUP BY
    s.country_code
ORDER BY
    total_no_stores DESC
LIMIT 3;

-- +----------+-----------------+
-- | country  | total_no_stores |
-- +----------+-----------------+
-- | GB       |             265 |
-- | DE       |             141 |
-- | US       |              34 |
-- +----------+-----------------+

-- Which locations have the most stores.

SELECT
    s.locality,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details s
GROUP BY
    s.locality
ORDER BY
    total_no_stores DESC, s.locality
LIMIT 7;

-- +-------------------+-----------------+
-- |     locality      | total_no_stores |
-- +-------------------+-----------------+
-- | Chapletown        |              14 |
-- | Belper            |              13 |
-- | Bushley           |              12 |
-- | Exeter            |              11 |
-- | High Wycombe      |              10 |
-- | Arbroath          |              10 |
-- | Rutherglen        |              10 |
-- +-------------------+-----------------+

-- Which months have produced the most sales.

SELECT
    ROUND(SUM(o.product_quantity * p.product_price)::numeric, 2) AS total_sales,
    d.month
FROM
    orders_table o
JOIN
    dim_date_times d ON o.date_uuid = d.date_uuid
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    d.month
ORDER BY
    total_sales DESC
LIMIT 6;

-- +-------------+-------+
-- | total_sales | month |
-- +-------------+-------+
-- |   673295.68 |     8 |
-- |   668041.45 |     1 |
-- |   657335.84 |    10 |
-- |   650321.43 |     5 |
-- |   645741.70 |     7 |
-- |   645463.00 |     3 |
-- +-------------+-------+

-- How many products were sold and the amount of sales made for online and offline purchases.

WITH sales AS (
    SELECT 
        o.store_code,
        s.store_type,
        o.product_quantity
    FROM 
        orders_table o
    JOIN 
        dim_store_details s ON o.store_code = s.store_code
    )

SELECT 
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE 
        WHEN store_type IN ('Mall Kiosk', 'Super Store', 'Local', 'Outlet') THEN 'offline'
        WHEN store_type = 'Web Portal' THEN 'Web'
    END AS location
FROM 
    sales
GROUP BY 
    CASE 
        WHEN store_type IN ('Mall Kiosk', 'Super Store', 'Local', 'Outlet') THEN 'offline'
        WHEN store_type = 'Web Portal' THEN 'Web'
    END
ORDER BY 
    location DESC;

-- +------------------+-------------------------+----------+
-- | numbers_of_sales | product_quantity_count  | location |
-- +------------------+-------------------------+----------+
-- |            26957 |                  107739 | Web      |
-- |            93166 |                  374047 | Offline  |
-- +------------------+-------------------------+----------+

-- The total and percentage of sales coming from each of the different store types.

WITH total_sales_per_store_type AS (
    SELECT
        s.store_type,
        ROUND(SUM(p.product_price * o.product_quantity)::NUMERIC, 2) AS total_sales_value
    FROM
        orders_table o
    JOIN
        dim_products p ON o.product_code = p.product_code
    JOIN
        dim_store_details s ON o.store_code = s.store_code
    GROUP BY
        s.store_type
    ),

total_sales AS (
    SELECT
        SUM(total_sales_value) AS total_sales_sum
    FROM
        total_sales_per_store_type
    )

SELECT
    ts.store_type,
    ts.total_sales_value AS total_sales,
    ROUND((ts.total_sales_value / ts_total.total_sales_sum * 100)::NUMERIC, 2) AS percentage_total
FROM
    total_sales_per_store_type ts
JOIN
    total_sales ts_total ON 1=1
ORDER BY
	total_sales DESC;

-- +-------------+-------------+---------------------+
-- | store_type  | total_sales | percentage_total(%) |
-- +-------------+-------------+---------------------+
-- | Local       |  3440896.52 |               44.87 |
-- | Web portal  |  1726547.05 |               22.44 |
-- | Super Store |  1224293.65 |               15.63 |
-- | Mall Kiosk  |   698791.61 |                8.96 |
-- | Outlet      |   631804.81 |                8.10 |
-- +-------------+-------------+---------------------+

-- Which months in which years have had the most sales historically.

SELECT
    ROUND(SUM(p.product_price * o.product_quantity)::NUMERIC, 2) AS total_sales,
    dt.year,
    dt.month
FROM
    dim_date_times dt
JOIN
    orders_table o ON dt.date_uuid = o.date_uuid
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    dt.month,
    dt.year
ORDER BY	
    total_sales DESC
LIMIT 10;

-- +-------------+------+-------+
-- | total_sales | year | month |
-- +-------------+------+-------+
-- |    27936.77 | 1994 |     3 |
-- |    27356.14 | 2019 |     1 |
-- |    27091.67 | 2009 |     8 |
-- |    26679.98 | 1997 |    11 |
-- |    26310.97 | 2018 |    12 |
-- |    26277.72 | 2019 |     8 |
-- |    26236.67 | 2017 |     9 |
-- |    25798.12 | 2010 |     5 |
-- |    25648.29 | 1996 |     8 |
-- |    25614.54 | 2000 |     1 |
-- +-------------+------+-------+

-- The staff numbers in each of the countries the company sells in.

SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- +---------------------+--------------+
-- | total_staff_numbers | country_code |
-- +---------------------+--------------+
-- |               13307 | GB           |
-- |                6123 | DE           |
-- |                1384 | US           |
-- +---------------------+--------------+

-- Which type of store is generating the most sales in Germany.

WITH total_sales_germany AS (
    SELECT
        s.store_type,
        ROUND(SUM(p.product_price * o.product_quantity)::NUMERIC, 2) AS total_sales_value,
		s.country_code
    FROM
        orders_table o
    JOIN
        dim_products p ON o.product_code = p.product_code
    JOIN
        dim_store_details s ON o.store_code = s.store_code
	WHERE 
		s.country_code = 'DE'
    GROUP BY
        s.store_type,
		s.country_code
    )
	
SELECT
	ts.total_sales_value AS total_sales,
    ts.store_type,
	ts.country_code
FROM
    total_sales_germany ts
ORDER BY
	total_sales ASC;

-- +--------------+-------------+--------------+
-- | total_sales  | store_type  | country_code |
-- +--------------+-------------+--------------+
-- |   198373.57  | Outlet      | DE           |
-- |   247634.20  | Mall Kiosk  | DE           |
-- |   384625.03  | Super Store | DE           |
-- |  1109909.59  | Local       | DE           |
-- +--------------+-------------+--------------+

-- The average time taken between each sale grouped by year.

WITH timestamp_first_next AS (
	SELECT
	    year,
	    TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS first_timestamp,
		LEAD(TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS')) OVER (ORDER BY year, month, day, timestamp) AS next_timestamp
	FROM 
		dim_date_times d
	JOIN
		orders_table o ON d.date_uuid = o.date_uuid 
	),
	
average_differences_per_year AS (
	SELECT 
		year,
		AVG(EXTRACT(EPOCH FROM (next_timestamp - first_timestamp))) average_time_difference
	FROM 
		timestamp_first_next
	GROUP BY 
		year
	)

SELECT 
	year,
	CONCAT(
        '"hours": ', FLOOR(average_time_difference / 3600), ', ',
        '"minutes": ', FLOOR((average_time_difference % 3600) / 60), ', ',
        '"seconds": ', FLOOR(average_time_difference % 60), ', ',
        '"milliseconds": ', ROUND((average_time_difference - FLOOR(average_time_difference)) * 1000)
    ) AS formatted_averages
FROM
	average_differences_per_year
ORDER BY
	average_time_difference DESC;

--  +------+-------------------------------------------------------+
--  | year |                           actual_time_taken           |
--  +------+-------------------------------------------------------+
--  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
--  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
--  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
--  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
--  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
--  +------+-------------------------------------------------------+