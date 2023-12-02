import yaml
from sqlalchemy import create_engine, inspect
class DatabaseConnector():

    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, filename='aws_db_creds.yaml'):
        with open(filename, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
        
    def init_db_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = self.db_creds['HOST']
        USER = self.db_creds['USER']
        PASSWORD = self.db_creds['PASSWORD']
        PORT = self.db_creds['PORT']
        DATABASE = 'postgres'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        inspector = inspect(self.engine)
        for table_name in inspector.get_table_names():
            print(table_name)

    def upload_to_db(self, df, table_name):
        local_creds = self.read_db_creds(filename='local_db_creds.yaml')
        print(local_creds)
        engine = create_engine(f"postgresql+psycopg2://{local_creds['USER']}:{local_creds['PASSWORD']}@localhost:5432/sales_data")
        df.to_sql(table_name, engine) 


        

database_connector = DatabaseConnector()

if __name__ == "__main__":
    database_connector.list_db_tables()

        