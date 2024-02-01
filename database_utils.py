import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector():
    """    
    This class can be used to connect to local and cloud-based postgresql databases by using a SQLAlchemy engine.

    Attributes:
            db_creds (dict): The cloud-based database credentials returned from the read_db_creds function
            engine (sqlalchemy.engine.Engine): Interface for interacting with cloud-based database
    """
    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, filename='aws_db_creds.yaml'):
        """
        This function is used to read SQL database credentials, taking the aws credentials in the directory as
        default though can be used for any yaml file with credentials.

        Args:
                filename (str): The yaml file to read credentials from
        Returns:
                db_creds (dict): The database credentials as a dictionary
        """       
        try:
            with open(filename, 'r') as file:
                db_creds = yaml.safe_load(file)
            print(f"File '{filename}' successfully read.")
            return db_creds
        except FileNotFoundError:
            print(f"File '{filename}' not found.")
            return None 
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    def init_db_engine(self):
        """
        This function is used to initialise a SQLAlchemy database engine to interact with an AWS RDS database.

        Returns:
                engine (sqlalchemy.engine.Engine): Interface for interacting with database
        """       
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = self.db_creds['HOST']
        USER = self.db_creds['USER']
        PASSWORD = self.db_creds['PASSWORD']
        PORT = self.db_creds['PORT']
        DATABASE = 'postgres'

        try:
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
            engine.connect()
            print("Database engine successfully connected.")
        except Exception as e:
            print(f"Could not connect to database because of the error: {e}")

        return engine
    
    def list_db_tables(self):
        """
        This function is used to list the tables in the AWS RDS database instance to help the user decide which
        they want to extract data from.
        """
        inspector = inspect(self.engine)
        print("\nThe tables in the database are as follows:\n")
        for table_name in inspector.get_table_names():
            print(table_name)

    def upload_to_db(self, df, table_name):
        """
        This function is used to upload data from a pandas dataframe to a new table in a local postgresql
        database.

        Args:
                df (pandas.DataFrame): The dataframe which contains the data to upload to database
                table_name (str): The name of the new table created by the user to upload the data to
        """               
        try:
            local_creds = self.read_db_creds(filename='local_db_creds.yaml')
            engine = create_engine(f"postgresql+psycopg2://{local_creds['USER']}:{local_creds['PASSWORD']}@localhost:5432/sales_data")
            df.to_sql(table_name, engine)
            print(f"Successfully uploaded data to {table_name} in the database.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
      
database_connector = DatabaseConnector()
   