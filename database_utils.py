import yaml
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError


class DatabaseConnector:
    """A class that facilitates connection to a database.

    Attributes:
        db_file (str): Stores the path to the YAML file.
        conn: Stores a database connection object.
    """

    def __init__(self, db_file):
        """Initialise the DatabaseConnector with a database file.

        Args: 
            db_file (str): the path to the database file
        """
        self.db_file = db_file
        self.engine = None
        self.conn = None

    def read_db_creds(self, creds_file):
        """Read database credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file.
        Return: 
            dict - database credentials
        """
        with open(creds_file, 'r') as file:
            try:
                creds = yaml.safe_load(file)

                print("Database credentials read successfully.")
                return creds
            except yaml.YAMLError as e:
                print(f"Error reading YAML file: {e}")
                return None
            
    def init_db_engine(self):
        """Initialise database engine.
        """
        creds = self.read_db_creds(self.db_file)
        if not creds:
            print("Failed to read database credentials.")
            return None
        username = creds['RDS_USER']
        password = creds['RDS_PASSWORD']
        host = creds['RDS_HOST']
        port = creds['RDS_PORT']
        database = creds['RDS_DATABASE']
        db_url = f'postgresql://{username}:{password}@{host}:{port}/{database}'

        try:
            self.engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
            self.conn = self.engine.connect()

            print("Database connection established successfully.")
            return self.engine
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
    
    def list_db_tables(self):
        """List all tables in the connected database.
        """
        try:
            if not self.engine:
                self.init_db_engine()
            if not self.engine:
                print("Database engine not initialized.")
                return []

            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            table_names = metadata.tables.keys()
            
            print(f"Tables in the database: {list(table_names)}")
            return table_names
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def upload_to_db(self, df, table_name):
        """Upload DataFrame to the specified table in the database."""
        if not self.engine:
            self.init_db_engine()
        if not self.engine:
            print("Database engine not initialized.")
            return

        metadata = MetaData()
        try:
            table = Table(table_name, metadata, autoload_with=self.engine)
            with self.engine.connect() as connection:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)
            print(f"Data uploaded successfully to table '{table_name}'")
        except SQLAlchemyError as e:
            print(f"Error uploading data to table '{table_name}': {str(e)}")

if __name__ == "__main__":
    creds_file = r'c:/Users/safi-/OneDrive/Occupation/AiCore/AiCore Training/PROJECTS/' \
                                    'Multinational Retail Data Centralisation Project/multinational-retail-data-centralisation855/db_creds.yaml'
    db_connector = DatabaseConnector(creds_file)
    tables = db_connector.list_db_tables()

    print("Existing tables:", ', '.join(tables))