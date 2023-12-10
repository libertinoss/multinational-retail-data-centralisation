
-- 5. What percentage of sales come through each type of store?

SELECT
    store_type,
    ROUND(SUM(product_price_gbp * product_quantity)::numeric, 2) AS total_sales,
    100 * SUM(product_price_gbp * product_quantity) / total.sum_all_sales AS percentage_total
FROM
    orders_table AS o
INNER JOIN 
    dim_store_details AS s ON o.store_code = s.store_code
INNER JOIN
    dim_products AS p ON o.product_code = p.product_code
CROSS JOIN (
 SELECT SUM(product_price_gbp * product_quantity) as sum_all_sales
    FROM orders_table as o 
    INNER JOIN dim_products as p ON o.product_code = p.product_code
) AS total
GROUP BY
    store_type, total.sum_all_sales
ORDER BY
    total_sales DESC;

-------------------------------------------------------------
WITH cte as (
SELECT
	year, 
    month,
    day,
	date_timestamp,
    --interval
	LEAD(date_timestamp,1) OVER (
	ORDER BY year, month, day, date_timestamp) -
	date_timestamp AS difference
FROM
	avgtesting
WHERE
   year = '1994'
ORDER BY
    date_timestamp)

SELECT 
    year,
    sum(difference)/count(difference) as avg_time_between_each_sale
FROM
    cte
GROUP BY
    year




ALTER TABLE test_table
ALTER COLUMN difference type interval using difference::interval

ALTER TABLE test_table2
ALTER COLUMN difference type time using difference::time




SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'test_table2'


