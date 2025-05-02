import os
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

engine = None

def start_sql_pipeline() -> bool:
    global engine
    try:
        load_dotenv()

        username = os.getenv('RDS_USERNAME')
        password = os.getenv('RDS_PASSWORD')
        host = os.getenv('RDS_HOST')
        port = os.getenv('RDS_PORT', 3306)
        database = os.getenv('RDS_DATABASE')

        url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'

        engine = create_engine(url)
        connection = engine.connect()
        connection.close()

        print("Connection to RDS successful.")
        return True

    except Exception as e:
        print(f"Failed to connect to RDS: {e}")
        engine = None
        return False

def replace_table_sql_data(df, table_title):
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_title)

    with engine.connect() as conn:
        if table_exists:
            conn.execute(text(f"DELETE FROM {table_title}"))
            conn.commit()
    
    df.to_sql(name=table_title, con=engine, if_exists='append', index=False)
    print(f"successfully replaced {table_title} db table")

def append_sql_data(df: pd.DataFrame, table_title: str):
    df.to_sql(name=table_title, con=engine, if_exists='append', index=False)
    print(f"successfully appended {table_title} df into db")

def export_tables_to_csv():
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    output_dir = os.path.join(desktop, os.getenv('RDS_DATABASE'))

    os.makedirs(output_dir, exist_ok=True)

    table_names = ["USERS", "REVIEWS", "GAMES"]

    try:
        for table in table_names:
            df = pd.read_sql_table(table, engine)

            output_path = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(output_path, index=False)

            print(f"Successfully exported {table} to {output_path}")
    except Exception as e:
        print(f"Failed to export tables: {e}")
