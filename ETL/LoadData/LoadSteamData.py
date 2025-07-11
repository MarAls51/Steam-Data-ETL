import os
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv


class Database:
    def __init__(self):
        """
        Initialize the Database class by loading environment variables and setting connection parameters.
        """
        load_dotenv()

        self.username = os.getenv('RDS_USERNAME')
        self.password = os.getenv('RDS_PASSWORD')
        self.host = os.getenv('RDS_HOST')
        self.port = int(os.getenv('RDS_PORT', 5432))
        self.database = os.getenv('RDS_DATABASE')

        self.engine = None

    def connect(self) -> bool:
        """
        Establish a connection to the PostgreSQL database using SQLAlchemy.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            url = f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
            self.engine = create_engine(url)

            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            print("Connection to RDS successful.")
            return True

        except Exception as e:
            print(f"Failed to connect to RDS: {e}")
            self.engine = None
            return False

    def replace_table(self, df: pd.DataFrame, table_name: str):
        """
        Replace all records in a database table with new data from a DataFrame.

        Args:
            df (pd.DataFrame): The new data to insert.
            table_name (str): The name of the table to replace.
        """
        if self.engine is None:
            print("Engine not initialized. Call connect() first.")
            return

        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            with self.engine.connect() as conn:
                conn.execute(text(f'DELETE FROM "{table_name}"'))
                conn.commit()

        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        print(f"Replaced data in table '{table_name}'.")

    def append_table(self, df: pd.DataFrame, table_name: str, pk_column: str = 'steamid'):
        """
        Append new records to a table, skipping rows that already exist based on a primary key column.

        Args:
            df (pd.DataFrame): The data to insert.
            table_name (str): The name of the target table.
            pk_column (str): The column used to identify existing records (default: 'steamid').
        """
        if self.engine is None:
            print("Engine not initialized. Call connect() first.")
            return
        
        existing_ids = pd.read_sql(f'SELECT {pk_column} FROM "{table_name}"', self.engine)[pk_column].astype(str).tolist()
        df[pk_column] = df[pk_column].astype(str)
        
        df_filtered = df[~df[pk_column].isin(existing_ids)]
        
        if df_filtered.empty:
            print(f"No new rows to insert into '{table_name}'.")
            return
        
        df_filtered.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        print(f"Appended {len(df_filtered)} new rows to table '{table_name}'.")

    def export_tables_to_csv(self, output_dir=None):
        """
        Export predefined database tables (GAMES, REVIEWS, USERS) to CSV files.

        Args:
            output_dir (str, optional): The directory to save CSV files. If not provided, exports to Desktop.
        """
        if self.engine is None:
            print("Engine not initialized. Call connect() first.")
            return

        if output_dir is None:
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            output_dir = os.path.join(desktop, self.database or "rds_export")

        os.makedirs(output_dir, exist_ok=True)

        table_names = ["USERS", "REVIEWS", "GAMES"]

        try:
            for table in table_names:
                df = pd.read_sql_table(table, self.engine)
                output_path = os.path.join(output_dir, f"{table}.csv")
                df.to_csv(output_path, index=False)
                print(f"Exported {table} to {output_path}")
        except Exception as e:
            print(f"Failed to export tables: {e}")
