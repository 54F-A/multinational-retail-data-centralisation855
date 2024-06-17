from sqlalchemy import select, MetaData, Table
import pandas as pd
import database_utils


class DataExtractor:
    """A class to extract data from a relational database.

    Attributes:
        db_connector (DatabaseConnector): An instance of DatabaseConnector.
    """

    def __init__(self, db_connector):
        """Initialises the DataExtractor instance with a database connector.

        Args:
            db_connector (DatabaseConnector): An instance of DatabaseConnector.
        """
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """Reads data from a specified table in the database and returns it as a DataFrame.

        Args:
            table_name (str): Name of the table.

        Returns:
            DataFrame: DataFrame containing the data from the specified table.
        """
        engine = self.db_connector.init_db_engine()
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        query = select(table.columns)

        with engine.connect() as connection:
            result_proxy = connection.execute(query)
            df = pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())

        return df

if __name__ == "__main__":
    creds_file = r'c:/Users/safi-/OneDrive/Occupation/AiCore/AiCore Training/PROJECTS/' \
                 'Multinational Retail Data Centralisation Project/multinational-retail-data-centralisation855/db_creds.yaml'
    db_connector = database_utils.DatabaseConnector(creds_file)

    tables = db_connector.list_db_tables()
    data_extractor = DataExtractor(db_connector)
    table_name = 'legacy_users'
    df = data_extractor.read_rds_table(table_name)

    print("Tables in the database:")
    for table in tables:
        print(table)