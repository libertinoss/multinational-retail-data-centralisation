--- How many stores does the business have and in which countries?

SELECT 
    country_code, COUNT(*) as total_no_stores
FROM 
    dim_store_details
WHERE
    store_code NOT LIKE 'WEB%'
GROUP BY 
    country_code;

--- Which locations have the most stores?

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

--- Which month has produced the most sales?

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

--- What is the split between online and offline sales?

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

--- What percentage of sales come through each type of store?

WITH cte AS (
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
    dim_products_ as p ON o.product_code = p.product_code
GROUP BY store_type)

SELECT
    store_type,
    total_sales,
    ROUND((total_sales/sum_all_sales * 100)::numeric,2) as "percentage_total(%)"
FROM 
    cte
ORDER BY total_sales DESC

--- Historically which months in which years have had the most sales?

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

--- What is the staff headcount in each country?

SELECT
    SUM(staff_numbers) as total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY total_staff_numbers DESC

--- Which German store type is selling the most?

SELECT
    ROUND(SUM(product_price_gbp * product_quantity)::numeric,2) as total_sales,
    store_type,
    country_code
FROM
    orders_table as o
INNER JOIN
    dim_products as p ON o.product_code = p.product_code
INNER JOIN 
    dim_store_details_ as s ON o.store_code = s.store_code
GROUP BY
    store_type, s.country_code 
HAVING
    s.country_code = 'DE'
ORDER BY 
    total_sales 

--- What is the average time taken between each sale, grouped by year?

-- Start by creating column with a full timestamp created from the year, month, day and timestamp columns

ALTER TABLE dim_date_times
    ADD COLUMN date_time_timestamp TEXT; -- start with text so it can be easily manipulated

UPDATE dim_date_times -- combine data from columns into one
    SET date_time_timestamp = year || '-' || month || '-' || day || ' ' || timestamp;

ALTER TABLE dim_date_times
    ALTER COLUMN date_time_timestamp TYPE TIMESTAMP USING date_time_timestamp::timestamp;

-- Now create table which has a column for difference between every date_time_timestamp
-- Use lead function so each row contains the difference between the date_time_timestamp on that row and the next one (ordered chronologically)

CREATE TABLE ddt_with_intervals AS  -- create with intervals for easy querying
WITH cte AS (
    SELECT
        year, 
        month,
        day,
        date_time_timestamp,
        LEAD(date_time_timestamp, 1) OVER (ORDER BY date_time_timestamp) - date_time_timestamp AS difference
    FROM
        dim_date_times
)
SELECT * FROM cte;

/* Problem with this table is the last row of every year (ordered by date_time_timestamp) gives us the difference 
   between that date_time_timestamp and the first one of the following year. We don't want this one to be included
   as by definition we won't be looking at the average for that year anymore. Can fix this by inserting a NULL for 
   the max date_time_timestamp for each year (ie last row of every year)*/

UPDATE ddt_with_intervals AS d 
SET difference = NULL
FROM (
    SELECT year, MAX(date_time_timestamp) AS max_timestamp
    FROM ddt_with_intervals
    GROUP BY year
) AS subquery
WHERE d.year = subquery.year
AND d.date_time_timestamp = subquery.max_timestamp;

-- Now can simply get the average difference for the year and group by year for the correct answer

SELECT 
    year,
    AVG(difference) as actual_time_taken
FROM
    ddt_with_intervals
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT 5