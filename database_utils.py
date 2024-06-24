from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
import yaml


class DatabaseConnector:
    """A class to connect to and interact with a PostgreSQL database.

    Attributes:
        db_file (str): Path to the YAML file containing database credentials.
        engine: SQLAlchemy Engine object for database connection.
        conn: SQLAlchemy Connection object for database interaction.
    """

    def __init__(self, db_file):
        """Initialises the DatabaseConnector instance with the path to the database credentials file.

        Args:
            db_file (str): Path to the YAML file containing database credentials.
        """
        self.db_file = db_file
        self.engine = None
        self.conn = None

    def read_db_creds(self, creds_file):
        """Reads database credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.

        Returns:
            dict: Dictionary containing database credentials.
        """
        with open(creds_file, 'r') as file:
            try:
                creds = yaml.safe_load(file)

                print("Database credentials read successfully.")
                return creds
            except yaml.YAMLError as e:
                print(f"Error reading YAML file: {e}")
                return None
            
    def init_db_engine(self, db_type='source'):
        """Initialises the database connection engine based on the provided database type.

        Args:
            db_type (str): Type of database to connect to. Default is 'source'.

        Returns:
            sqlalchemy.engine.base.Engine: SQLAlchemy Engine object.
        """
        creds = self.read_db_creds(self.db_file)
        if not creds:
            print("Failed to read database credentials.")
            return None
        
        if db_type == "source":
            username = creds['RDS_USER']
            password = creds['RDS_PASSWORD']
            host = creds['RDS_HOST']
            port = creds['RDS_PORT']
            database = creds['RDS_DATABASE']
        elif db_type == "local":
            username = creds['LOCAL_USER']
            password = creds['LOCAL_PASSWORD']
            host = creds['LOCAL_HOST']
            port = creds['LOCAL_PORT']
            database = creds['LOCAL_DATABASE']
        else:
            return None

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
        """Lists all tables in the connected database.

        Returns:
            list: List of table names in the database.
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
            
            print(f"{list(table_names)}")
            return table_names
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def upload_to_db(self, df, table_name, db_type='source'):
        """Uploads a DataFrame to a specified table in the database.

        Args:
            df (DataFrame): DataFrame to upload.
            table_name (str): Name of the table to upload data into.
            db_type (str): Type of database to upload data to. Default is 'source'.
        """
        if not self.engine:
            self.init_db_engine(db_type)
        if not self.engine:
            print("Database engine not initialized.")
            return None

        metadata = MetaData()
        try:
            table = Table(table_name, metadata, autoload_with=self.engine)
            with self.engine.connect() as connection:
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)

            print(f"Data uploaded successfully to table '{table_name}'")
        except SQLAlchemyError as e:
            print(f"Error uploading data to table '{table_name}': {str(e)}")
