# import pandas as pd
# import numpy as np
# import sys
# import logging
# import os
# import time


# from dotenv import load_dotenv
# load_dotenv('.env')
# from sqlalchemy import create_engine

# user = os.getenv("DB_USER")
# password = os.getenv("DB_PASSWORD")
# host = os.getenv("DB_HOST")
# port = os.getenv('DB_PORT')
# dbname = os.getenv("DB_NAME")

# db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# logging.basicConfig(
#     filename=r'F:\projectvendor\logs\injestion_db.logs',
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     filemode='a'
# )

# engine = create_engine(db_url)

# def ingest_db(df,table_name,engine):
#     df.to_sql(table_name, engine, if_exists='replace', index=False)
#     print(f"Data ingested into table {table_name} successfully.")


# def load_raw_data():
#     'this function will load the raw data from the data folder and ingest it into the database'
#     start=time.time()
#     for file in os.listdir("data"):
#         if '.csv' in file:
#             df=pd.read_csv(f"data/{file}")
#             logging.info(f'Ingesting {file} into db')
#             ingest_db(df,file[:-4],engine)
#     end=time.time()
#     total_time=(end-start)/60
#     logging.info("---------Ingestion Completed--------- ")
#     logging.info(f"Total time taken to ingest data: {total_time} minutes")


# if __name__ == "__main__":
#     load_raw_data()
#     logging.info("Ingestion script executed successfully.")
#     print("Ingestion script executed successfully.")   

"""
Data Ingestion Script: Loads all CSV files from 'data/' into PostgreSQL tables.
"""

# import os
# import sys
# import time
# import logging
# import pandas as pd

# from dotenv import load_dotenv
# from sqlalchemy import create_engine
# from sqlalchemy.exc import SQLAlchemyError

# # Load environment variables
# load_dotenv('.env')

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("F:/projectvendor/logs/injestion_db.logs"),
#         logging.StreamHandler(sys.stdout)
#     ]
# )

# # Read DB config from .env
# DB_CONFIG = {
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME")

# }

# # Validate required env vars
# if not all(DB_CONFIG.values()):
#     missing = [k for k, v in DB_CONFIG.items() if v is None]
#     logging.error(f"Missing required environment variables: {missing}")
#     sys.exit(1)

# # Build DB URL
# db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# try:
#     engine = create_engine(db_url)
#     with engine.connect() as conn:
#         logging.info("Successfully connected to the database.")
# except SQLAlchemyError as e:
#     logging.error(f" Failed to connect to the database: {e}")
#     sys.exit(1)

# def ingest_df_to_postgres(df, table_name, engine):
#     """
#     Ingest a DataFrame into a PostgreSQL table.
#     """
#     try:
#         df.to_sql(table_name, engine, if_exists='replace', index=False)
#         logging.info(f"Data successfully ingested into '{table_name}'")
#     except Exception as e:
#         logging.error(f" Failed to ingest data into '{table_name}': {e}")
#         raise

# def load_raw_data(data_folder="data"):
#     """
#     Main function to load all CSVs from a folder into PostgreSQL.
#     """
#     start_time = time.time()

#     if not os.path.exists(data_folder):
#         logging.error(f" Data directory '{data_folder}' does not exist.")
#         return

#     csv_files = [f for f in os.listdir(data_folder) if f.lower().endswith(".csv")]
#     if not csv_files:
#         logging.warning(" No CSV files found in data directory.")
#         return

#     logging.info(f" Found {len(csv_files)} CSV files to process.")

#     for file in csv_files:
#         full_path = os.path.join(data_folder, file)
#         table_name = os.path.splitext(file)[0]

#         try:
#             logging.info(f"Reading file: {file}")
#             df = pd.read_csv(full_path)
#             ingest_df_to_postgres(df, table_name, engine)
#         except Exception as e:
#             logging.error(f" Error processing file '{file}': {e}")

#     total_minutes = (time.time() - start_time) / 60
#     logging.info(f"Ingestion completed in {total_minutes:.2f} minutes.")

# if __name__ == "__main__":
#     logging.info("Starting data ingestion process...")
#     load_raw_data()
#     logging.info(" Data ingestion script completed successfully.")



"""
Data Ingestion Script: Loads all CSV files from 'data/' into PostgreSQL tables.
"""

import os
import sys
import time
import logging
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv('.env')

# Configure logging with UTF-8 support
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("F:/projectvendor/logs/injestion_db.logs", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Read DB config from .env
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME")
}

# Validate required env vars
if not all(DB_CONFIG.values()):
    missing = [k for k, v in DB_CONFIG.items() if v is None]
    logging.error(f"Missing required environment variables: {missing}")
    sys.exit(1)

# Build DB URL
db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

try:
    # Use pool_pre_ping to handle dropped connections
    engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
    with engine.connect() as conn:
        logging.info("Successfully connected to the database.")
except SQLAlchemyError as e:
    logging.error(f"Failed to connect to the database: {e}")
    sys.exit(1)

def ingest_df_to_postgres(df, table_name, engine, chunksize=10000):
    """
    Ingest a DataFrame into a PostgreSQL table in chunks.
    """
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=chunksize)
        logging.info(f"Data successfully ingested into '{table_name}'")
    except Exception as e:
        logging.error(f"Failed to ingest data into '{table_name}': {e}")
        raise

def load_raw_data(data_folder="data"):
    """
    Main function to load all CSVs from a folder into PostgreSQL.
    """
    start_time = time.time()

    if not os.path.exists(data_folder):
        logging.error(f"Data directory '{data_folder}' does not exist.")
        return

    csv_files = [f for f in os.listdir(data_folder) if f.lower().endswith(".csv")]
    if not csv_files:
        logging.warning("No CSV files found in data directory.")
        return

    logging.info(f"Found {len(csv_files)} CSV files to process.")

    for file in csv_files:
        full_path = os.path.join(data_folder, file)
        table_name = os.path.splitext(file)[0]

        try:
            logging.info(f"Reading file: {file}")
            df = pd.read_csv(full_path)
            logging.info(f"Loaded {len(df)} rows from '{file}'")
            ingest_df_to_postgres(df, table_name, engine)
        except Exception as e:
            logging.error(f"Error processing file '{file}': {e}")

    total_minutes = (time.time() - start_time) / 60
    logging.info(f"Ingestion completed in {total_minutes:.2f} minutes.")

if __name__ == "__main__":
    logging.info("Starting data ingestion process...")
    load_raw_data()
    logging.info("Data ingestion script completed successfully.")