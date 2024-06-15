from sqlalchemy import select, MetaData, Table
import pandas as pd
import database_utils


class DataExtractor:
    """A class that extracts data from different data sources.

    Attributes:
        db_connector: Stores an instance of database_utils.DatabaseConnector.
    """

    def __init__(self, db_connector):
        """Initialises an instance (object) of the class.

        Args:
            db_connector: Assigns the value of db_connector to an instance attribute.
        """
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """Read data from the specified table into a pandas DataFrame.

        Args:
            table_name (str): Name of the table to read from.
        Returns:
            pandas.DataFrame: DataFrame containing the table data.
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