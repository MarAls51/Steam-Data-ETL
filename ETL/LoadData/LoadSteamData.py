import os
import pandas as pd
from sqlalchemy import create_engine, text
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
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM {table_title}"))
        conn.commit()
    
    df.to_sql(name=table_title, con=engine, if_exists='append', index=False)
    print("successfully replaced db table")

def append_sql_data(df: pd.DataFrame, table_title: str):
    df.to_sql(name=table_title, con=engine, if_exists='append', index=False)
    print("successfully appended df into db")