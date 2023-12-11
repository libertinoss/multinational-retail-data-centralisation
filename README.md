# Multinational Retail Data Centralisation

## Outline
This is a project which involves extracting and manipulating the sales data of a fictional retailer. The data is spread across many different sources, making it difficult to access or analyse, so it is collated and loaded into a PostgreSQL database that acts as a single source of truth. The database is then queried for various business metrics.
The aim of this project was to create a fully functional data pipeline and improve my knowledge of various key technologies, such as **Python (Pandas, Boto3, SQLAlchemy)**, **PostgreSQL** and **AWS**.

## Installation and Initialisation
This project was created and tested using Python 3.11.4 and PostgreSQL 16. The entire data pipeline (from data extraction through to cleaning and uploading) can be executed using the **__main__.py** file, and the relevant SQL queries in terms of creating the database schema and obtaining a range of business metrics are in the **sql_queries/** folder in the directory. 

**Before executing any code a local PostgreSQL database names "sales_data" should be created!**

## Execution Workflow
As mentioned previously, running the **__main__.py** script executes the entire data pipeline. To add clarity for the user, there are status updates and relevant messages displayed throughout the execution of each individual extraction, cleaning and uploading function so it is clear what is happening at every stage. There is also basic error handling for common issues that might occur, indicating what is causing the problem. The code for data cleaning includes numerous print statements which indicate the overall workflow and logic of how each individual dataset was cleaned, but this is only displayed when the **__data_cleaning.py__** script is run directly so as to prevent unnecessary output clutter.

The files in the **sql_queries/** folder contain numerous queries that are clearly annotated as to what they are achieving in terms of database alterations and obtaining business metrics. They should be run in the order that they have been written in the .sql files to achieve the desired results. 

## File structure
```
├── __main__.py
├── database_utils.py
├── data_extraction.py
├── data_cleaning.py
├── sql_queries
|    ├── creating_database_schema.sql
|    └── querying_data_for_metrics.sql
└── extracted_data
    ├── card_details.csv
    ├── card_details.pdf
    ├── event_details.json
    ├── order_details.csv
    ├── product_details.csv
    ├── product_details_weights_converted.csv
    ├── store_details.csv
    └── user_details.csv

```
### Python Files (.py)
- *\__main\__.py* - Main script for the complete data pipeline
- *database_utils.py* - Code for establishing connection interface with local and cloud-based SQL databases
- *data_extraction.py* - Code for extracting relevant datasets from a range of sources online (Amazon RDS instance, S3 bucket, pdf table, AWS API endpoint)
- *data_cleaning.py* - Code for cleaning each dataset with a variety of techniques within the Pandas library

### SQL Files (.sql)
- *creating_database_schema.sql* - SQL queries for creating database schema, including setting data types and primary and foreign key constraints
- *querying_data_for_metrics.sql* - SQL queries for obtaining a range of business metrics surrounding sales through the usage of aggregate functions, joins, subqueries etc
  
### Extracted Data Files
These are the files containing the raw data from the numerous sources mentioned above, which are then used by the functions in the **data_extraction.py** module. They are downloaded automatically into this location upon running the **\__main\__.py** script but are included here for clarity, as some of the extraction methods require credentials that are not included in this remote directory.

## License information
This is free and unencumbered software released into the public domain.




