-- Changing orders table columns to correct data types

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'orders_table';

ALTER TABLE orders_table
    DROP COLUMN index;

ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE TEXT,
    ALTER COLUMN store_code TYPE TEXT,
    ALTER COLUMN product_code TYPE TEXT;

SELECT --getting maximum lengths for columns to be set as varchar
    (MAX(LENGTH(card_number))) AS max_card_number_length,
    (MAX(LENGTH(store_code))) AS max_store_code_length,
    (MAX(LENGTH(product_code))) AS max_product_code_length
FROM
    orders_table

ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
    ALTER COLUMN product_quantity TYPE SMALLINT

-- Changing dim_users table columns to correct data types

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_users';

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

ALTER TABLE dim_users
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN join_date TYPE DATE;

-- Changing dim_store_details table columns to correct data types

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_store_details';


UPDATE dim_store_details
    SET longitude = NULL,
        latitude = NULL
    WHERE
        store_type = 'Web Portal'

/*
UPDATE dim_store_details -- stripping staff_numbers column of characters so it can be cast to smallint
SET staff_numbers = regexp_replace(staff_numbers, '[^0-9]+', '', 'g');*/

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN store_type TYPE VARCHAR (255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::float,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

SELECT (MAX(LENGTH(store_code))) FROM dim_store_details;

ALTER TABLE dim_store_details
    ALTER COLUMN store_code TYPE VARCHAR(12);

UPDATE dim_store_details
    SET address = 'N/A',
        locality = 'N/A'
    WHERE
        store_type = 'Web Portal'

-- Making changes to dim_products table by adding weight_class column

ALTER TABLE dim_products
    ADD COLUMN weight_class TEXT

UPDATE dim_products
    SET weight_class = CASE
        WHEN weight_kg < 2 THEN 'Light'
        WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
        WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
        WHEN weight_kg >= 140 THEN 'Truck_Required'
    END;

SELECT * FROM dim_products WHERE weight_class IS NULL; --quick check to see all nulls converted

-- Changing dim_products table columns to correct data types

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_products';

SELECT
    (MAX(LENGTH("EAN"))) AS max_ean_length,
    (MAX(LENGTH(product_code))) AS max_product_code_length,
    (MAX(LENGTH(weight_class))) AS max_weight_class_length
FROM
    dim_products;

ALTER TABLE dim_products
    ALTER COLUMN product_price_gbp TYPE FLOAT USING product_price_gbp::float,
    ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::float,
    ALTER COLUMN "EAN" TYPE VARCHAR (17),
    ALTER COLUMN product_code TYPE VARCHAR (11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
    ALTER COLUMN weight_class TYPE VARCHAR(14);

ALTER TABLE dim_products --changing removed column to True/False to then convert to bool
    RENAME removed TO still_available;

UPDATE dim_products
    SET still_available = CASE
        WHEN still_available = 'Removed' THEN False
        WHEN still_available = 'Still_available' THEN True
    END;

ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE BOOLEAN using still_available::boolean;

-- Changing dim_date_times table columns to correct data types

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_date_times';

SELECT 
    (MAX(LENGTH(month))) AS max_month_length,
    (MAX(LENGTH(year))) AS max_year_length,
    (MAX(LENGTH(day))) AS max_day_length,
    (MAX(LENGTH(time_period))) AS max_time_period
FROM
    dim_date_times;

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR (2),
    ALTER COLUMN year TYPE VARCHAR (4),
    ALTER COLUMN day TYPE VARCHAR (2),
    ALTER COLUMN time_period TYPE VARCHAR (10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Changing dim_card_details table columns to correct data types
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'dim_card_details';

SELECT 
    (MAX(LENGTH(card_number))) AS max_card_number_length,
    (MAX(LENGTH(expiry_date))) AS max_expiry_date_length
FROM
    dim_card_details;

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR (19),
    ALTER COLUMN expiry_date TYPE VARCHAR (5),
    ALTER COLUMN date_payment_confirmed TYPE DATE;

-- Marking primary keys in each of the dimension tables

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

-- Marking foreign keys in orders_table

ALTER TABLE orders_table
    ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
    ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
    ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
    ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
    ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);













