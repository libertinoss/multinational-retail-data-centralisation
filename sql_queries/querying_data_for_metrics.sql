-- How many stores does the business have and in which countries?

SELECT 
    country_code, COUNT(*) as total_no_stores
FROM 
    dim_store_details
WHERE
    store_code NOT LIKE 'WEB%'
GROUP BY 
    country_code;

-- Which locations have the most stores?

SELECT 
    locality, COUNT(*) as total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
HAVING
    COUNT(*) >= 10
ORDER BY 
    total_no_stores DESC;

-- Which month has produced the most sales?

SELECT
    ROUND(SUM(product_price_gbp * product_quantity)::numeric,2) as total_sales, month
FROM
    orders_table as o
INNER JOIN
    dim_products as p ON o.product_code = p.product_code
INNER JOIN 
    dim_date_times as d ON o.date_uuid = d.date_uuid
GROUP BY
    month
ORDER BY 
    total_sales DESC 
LIMIT 6;

-- What is the split between online and offline sales?

SELECT
    CASE
        WHEN store_code LIKE 'WEB%' THEN 'Web'
        ELSE 'Offline'
    END
    AS location,
    COUNT(*) as numbers_of_sales,
    SUM(product_quantity) as product_quantity_count
FROM 
    orders_table
GROUP BY
    location

-- What percentage of sales come through each type of store?

WITH table_1 AS (
SELECT
    store_type,
    (SELECT SUM(product_price_gbp * product_quantity) 
    FROM orders_table as o 
    INNER JOIN dim_products as p ON o.product_code = p.product_code) as sum_all_sales,
    ROUND(SUM(product_price_gbp * product_quantity)::numeric,2) as total_sales
FROM
    orders_table as o
INNER JOIN 
    dim_store_details as s ON o.store_code = s.store_code
INNER JOIN
    dim_products as p ON o.product_code = p.product_code
GROUP BY store_type)

SELECT
    store_type,
    total_sales,
    ROUND((total_sales/sum_all_sales * 100)::numeric,2) as "percentage_total(%)"
FROM 
    table_1
ORDER BY total_sales DESC

-- Historically which months in which years have had the most sales?

SELECT
    ROUND(SUM(product_price_gbp * product_quantity)::numeric,2) as total_sales,
    year,
    month
FROM
    orders_table as o
INNER JOIN
    dim_products as p ON o.product_code = p.product_code
INNER JOIN 
    dim_date_times as d ON o.date_uuid = d.date_uuid
GROUP BY
    month, year
ORDER BY 
    total_sales DESC 
LIMIT 10

-- What is the staff headcount in each country?

SELECT
    SUM(staff_numbers) as total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY total_staff_numbers DESC

-- Which German store type is selling the most?

SELECT
    ROUND(SUM(product_price_gbp * product_quantity)::numeric,2) as total_sales,
    store_type,
    country_code
FROM
    orders_table as o
INNER JOIN
    dim_products as p ON o.product_code = p.product_code
INNER JOIN 
    dim_store_details as s ON o.store_code = s.store_code
GROUP BY
    store_type, s.country_code 
HAVING
    s.country_code = 'DE'
ORDER BY 
    total_sales 

-- What is the average time taken between each sale, grouped by year?


ALTER TABLE dim_date_times
    ALTER COLUMN timestamp TYPE TIME using timestamp::time;

SELECT
    timestamp,
    year,

FROM
    dim_date_times
ORDER BY
    year, timestamp 

-----    
ALTER TABLE dim_date_times
    DROP COLUMN difference
select * from dim_date_times


CREATE TABLE test_table3 AS
WITH cte AS (
    SELECT
        year, 
        month,
        day,
        date_time_timestamp,
        LEAD(date_time_timestamp, 1) OVER (ORDER BY year, month, day, date_time_timestamp) - date_time_timestamp AS difference
    FROM
        dim_date_times
)
SELECT * FROM cte;

-------
SELECT 
    year,
    AVG(difference) as avg_time_between_each_sale
FROM
    cte
GROUP BY
    year
ORDER BY
    avg_time_between_each_sale DESC
LIMIT 5

-------
ALTER TABLE dim_date_times
    ADD COLUMN date_time_timestamp TEXT;

UPDATE dim_date_times
    SET date_time_timestamp = year || '-' || month || '-' || day || ' ' || timestamp;

ALTER TABLE dim_date_times
    ALTER COLUMN date_time_timestamp TYPE TIMESTAMP USING date_time_timestamp::timestamp;

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE SMALLINT USING month::smallint,
    ALTER COLUMN year TYPE SMALLINT USING year::smallint,
    ALTER COLUMN day TYPE SMALLINT USING day::smallint;
---------

WITH cte as (
SELECT
	year, 
    month,
    day,
	date_time_timestamp,
	LEAD(date_time_timestamp,1) OVER (
		ORDER BY year, month, day, date_time_timestamp
	) - date_time_timestamp AS difference
FROM
	dim_date_times
--WHERE
  -- year = '2013' 
ORDER BY
    year, month, day, timestamp)

SELECT 
    year,
    AVG(difference) as avg_time_between_each_sale
FROM
    cte
GROUP BY
    year
ORDER BY
    avg_time_between_each_sale DESC
---------
select year, month, day, timestamp from dim_date_times order by year, month, day, timestamp

UPDATE test_table3 AS t
SET difference = NULL
FROM (
    SELECT year, MAX(date_time_timestamp) AS max_timestamp
    FROM test_table
    GROUP BY year
) AS subquery
WHERE t.year = subquery.year
AND t.date_time_timestamp = subquery.max_timestamp;

select * from test_table2 where difference is NULL

------------
SELECT
    year,
    AVG(difference) as actual_time_taken
FROM
    test_table3
--WHERE 
    --year = '2013'
GROUP BY
    year
ORDER BY
    actual_time_taken DESC

select * from dim_date_times

select * from orders_table




---------------









