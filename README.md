# Multinational Retail Data Centralisation

## Outline
This is a project which involves extracting and manipulating the sales data of a fictional retailer. The data is spread across many different sources, making it difficult to access or analyse, so it is collated and loaded into a PostgreSQL database that acts as a single source of truth. The database is then queried for various business metrics.
The aim of this project was to create a fully functional data pipeline and improve my knowledge of various key technologies, such as **Python (Pandas)**, **PostgreSQL** and **AWS (S3)**.

## Installation and Initialisation
This project was created and tested using Python 3.11.4 and PostgreSQL 16. The entire data pipeline (from data extraction through to cleaning and uplaoding) can be executed using the **__main__.py** file, and the relevant SQL queries in terms of creating the database schema and obtaining a range of business metrics are in the **sql_queries/** folder in the directory.

## Execution Workflow
As 

## File structure
```
├── __main__.py
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── extracted_data
│   ├── card_details.csv
│   ├── card_details.pdf
│   ├── event_details.json
│   ├── order_details.csv
│   ├── product_details.csv
│   ├── product_details_weights_converted.csv
│   ├── store_details.csv
│   └── user_details.csv
└── sql_queries
    ├── creating_database_schema.sql
    └── querying_data_for_metrics.sql
```
### Python Files

### SQL Files

### Extracted Data Files




- *hangman_game_main.py* - This file contains the main script for the game and it should be executed in order to play it
- *hangman_words.py* - This file contains a dictionary with 50 words for each category. These can be freely edited if desired
- *hangman_art.py* - This file contains all of the ASCII artwork used in the game and some functions which slightly embellish the user interface

## License information
This is free and unencumbered software released into the public domain.




