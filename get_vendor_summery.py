

import os
import sys
import time
import logging
import pandas as pd

from dotenv import load_dotenv
load_dotenv('.env')

from sqlalchemy import create_engine
from injestion import ingest_df_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("F:/projectvendor/logs/injestion_db.logs", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def create_vendor_summery(engine):
    """
    Create a summary of vendor sales and purchases.
    """
    try:
        query = """
        WITH FreightSummary AS (
    SELECT
        "VendorNumber",
        SUM("Freight") AS "FreightCost"
    FROM vendor_invoice
    GROUP BY "VendorNumber"
),

PurchaseSummary AS (
    SELECT
        p."VendorNumber",
        p."VendorName",
        p."Brand",
        p."Description",
        p."PurchasePrice",
        pp."Price" AS "ActualPrice",
        pp."Volume",
        SUM(p."Quantity") AS "TotalPurchaseQuantity",
        SUM(p."Dollars") AS "TotalPurchaseDollars"
    FROM purchases p
    JOIN purchase_prices pp
        ON p."Brand" = pp."Brand"
    WHERE p."PurchasePrice" > 0
    GROUP BY
        p."VendorNumber", p."VendorName", p."Brand", p."Description",
        p."PurchasePrice", pp."Price", pp."Volume"
),

SalesSummary AS (
    SELECT
        "VendorNo",
        "Brand",
        SUM("SalesQuantity") AS "TotalSalesQuantity",
        SUM("SalesDollars") AS "TotalSalesDollars",
        SUM("SalesPrice") AS "TotalSalesPrice",
        SUM("ExciseTax") AS "TotalExciseTax"
    FROM sales
    GROUP BY "VendorNo", "Brand"
)

SELECT
    ps."VendorNumber",
    ps."VendorName",
    ps."Brand",
    ps."Description",
    ps."PurchasePrice",
    ps."ActualPrice",
    ps."Volume",
    ps."TotalPurchaseQuantity",
    ps."TotalPurchaseDollars",
    ss."TotalSalesQuantity",
    ss."TotalSalesDollars",
    ss."TotalSalesPrice",
    ss."TotalExciseTax",
    fs."FreightCost"
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON ps."VendorNumber" = ss."VendorNo"
    AND ps."Brand" = ss."Brand"
LEFT JOIN FreightSummary fs
    ON ps."VendorNumber" = fs."VendorNumber"
ORDER BY ps."TotalPurchaseDollars" DESC;
        """
        
        vendor_sales_summary = pd.read_sql(query, engine)
        return vendor_sales_summary
    
    except Exception as e:
        logging.error(f"Error creating vendor summary: {e}")
        return None


def clean_data(vendor_sales_summery):
    """
    this function will clean the data
    """
    # change datatype of float
    vendor_sales_summery['Volume']= vendor_sales_summery['Volume'].astype('float64')
    vendor_sales_summery.fillna(0, inplace=True)


    vendor_sales_summery['VendorName']= vendor_sales_summery['VendorName'].str.strip()

    vendor_sales_summery['Description']= vendor_sales_summery['Description'].str.strip()

    vendor_sales_summery['GrossProfit'] = vendor_sales_summery['TotalSalesDollars'] - vendor_sales_summery['TotalPurchaseDollars']

    vendor_sales_summery['ProfitMargin']=vendor_sales_summery['GrossProfit'] / vendor_sales_summery['TotalSalesDollars']*100

    vendor_sales_summery['StockTurnover'] = vendor_sales_summery['TotalSalesQuantity'] / vendor_sales_summery['TotalPurchaseQuantity']

    vendor_sales_summery['SalesPurchaseRatio'] = vendor_sales_summery['TotalSalesDollars'] / vendor_sales_summery['TotalPurchaseDollars']

    return vendor_sales_summery


if __name__ == "__main__":
    logging.info("Starting vendor summary creation process...")

    # Create a database engine
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv('DB_PORT')
    dbname = os.getenv("DB_NAME")

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    engine = create_engine(db_url)

    logging.info("Database engine created successfully.")   
    start_time = time.time()
    vendor_sales_summery = create_vendor_summery(engine)
    if vendor_sales_summery is not None:                            
        logging.info("Vendor sales summary created successfully.")
        
        # Clean the data
        vendor_sales_summery = clean_data(vendor_sales_summery)
        logging.info("Data cleaned successfully.")

        # Send data to the table
        ingest_df_to_postgres(vendor_sales_summery, 'vendor_sales_summary', engine)
        logging.info("Data ingested into 'vendor_sales_summary' table successfully.")



        logging.info(f"Process completed in {time.time() - start_time:.2f} seconds.")
    else:
        logging.error("Failed to create vendor sales summary.")         
        sys.exit(1) 





    logging.info("Vendor summary creation process completed.")
    sys.exit(0)     

# This script creates a summary of vendor sales and purchases, cleans the data, and ingests it into a PostgreSQL database.
# It uses SQL queries to aggregate data from multiple tables and performs data cleaning operations before sending the
# final DataFrame to the database.

