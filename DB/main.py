import argparse
from dotenv import load_dotenv
from db.databaseschema import create_tables
from db.queries import run_queries

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Manage Steam ETL database")
    parser.add_argument(
        "--create-db",
        action="store_true",
        help="Create the database schema (tables)."
    )
    parser.add_argument(
        "--run-queries",
        action="store_true",
        help="Run the predefined SQL queries."
    )
    args = parser.parse_args()

    if args.create_db:
        create_tables()
    elif args.run_queries:
        run_queries()
        
if __name__ == "__main__":
    main()

