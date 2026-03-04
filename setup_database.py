# setup_databases.py
# Setup + populate MySQL + MongoDB for stock prices time-series
# Outputs clean, table-formatted verification for assignment

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import pymongo
from pymongo import MongoClient
import json

pd.set_option('display.max_columns', None) 
pd.set_option('display.width', 1000)

# CONFIGURATION


CSV_FILE = "portfolio_data.csv"

MYSQL_USER     = "root"
MYSQL_PASSWORD = "wagner"
MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_DB       = "stock_timeseries"

MONGO_URI      = "mongodb://localhost:27017/"
MONGO_DB_NAME  = "stock_timeseries"
MONGO_COLLECTION_NAME = "daily_prices"

# Connect

mysql_engine = sa.create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION_NAME]

print("=== Database Connections Established ===")

# Load Data

df = pd.read_csv(CSV_FILE, parse_dates=['Date'])
df = df.sort_values('Date').reset_index(drop=True)
df.columns = ['date', 'AMZN', 'DPZ', 'BTC', 'NFLX']

print(f"\nLoaded {len(df)} rows from CSV")
print(f"Date range: {df['date'].min().date()} → {df['date'].max().date()}")
print("\nSample data (first 3 rows):")
print(df.head(3))

# MySQL: Create Schema


try:
    with mysql_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("DROP TABLE IF EXISTS asset_prices;"))
        conn.execute(text("DROP TABLE IF EXISTS trading_dates;"))
        conn.execute(text("DROP TABLE IF EXISTS assets;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        conn.execute(text("""
            CREATE TABLE assets (
                asset_id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                full_name VARCHAR(100),
                asset_type ENUM('Stock', 'Cryptocurrency') DEFAULT 'Stock'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.execute(text("""
            CREATE TABLE trading_dates (
                date_id INT AUTO_INCREMENT PRIMARY KEY,
                trading_date DATE UNIQUE NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.execute(text("""
            CREATE TABLE asset_prices (
                price_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                date_id INT NOT NULL,
                asset_id INT NOT NULL,
                close_price DECIMAL(12,6) NOT NULL,
                FOREIGN KEY (date_id) REFERENCES trading_dates(date_id),
                FOREIGN KEY (asset_id) REFERENCES assets(asset_id),
                UNIQUE KEY unique_price (date_id, asset_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.commit()
    print("\nMySQL schema reset and recreated successfully.")
except Exception as e:
    print(f"\nSchema warning: {e} (continuing...)")

# MySQL

try:
    with mysql_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("DELETE FROM asset_prices;"))
        conn.execute(text("DELETE FROM trading_dates;"))
        conn.execute(text("DELETE FROM assets;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        conn.commit()
except Exception as e:
    print(f"Clear warning: {e}")

# Assets
assets_data = [
    {'symbol': 'AMZN', 'full_name': 'Amazon.com Inc.', 'asset_type': 'Stock'},
    {'symbol': 'DPZ', 'full_name': "Domino's Pizza Inc.", 'asset_type': 'Stock'},
    {'symbol': 'BTC', 'full_name': 'Bitcoin', 'asset_type': 'Cryptocurrency'},
    {'symbol': 'NFLX', 'full_name': 'Netflix Inc.', 'asset_type': 'Stock'}
]
pd.DataFrame(assets_data).to_sql('assets', mysql_engine, if_exists='append', index=False, method='multi')

# Dates
dates_df = pd.DataFrame({'trading_date': df['date'].dt.date.unique()})
dates_df.to_sql('trading_dates', mysql_engine, if_exists='append', index=False, method='multi')

# Prices
long_df = df.melt(id_vars=['date'], var_name='symbol', value_name='close_price')
long_df = long_df.rename(columns={'date': 'trading_date'})

with mysql_engine.connect() as conn:
    assets_map = pd.read_sql("SELECT asset_id, symbol FROM assets", conn)
    dates_map = pd.read_sql("SELECT date_id, trading_date FROM trading_dates", conn)
    dates_map['trading_date'] = pd.to_datetime(dates_map['trading_date'])

long_df = long_df.merge(assets_map, on='symbol')
long_df = long_df.merge(dates_map, on='trading_date')

prices_df = long_df[['date_id', 'asset_id', 'close_price']]
prices_df.to_sql('asset_prices', mysql_engine, if_exists='append', index=False, method='multi')

print(f"MySQL populated: {len(prices_df)} records ({len(dates_df)} dates × 4 assets)")


# MongoDB

mongo_collection.delete_many({})
records = [
    {
        "date": row['date'].isoformat(),
        "prices": {
            "AMZN": float(row['AMZN']),
            "DPZ": float(row['DPZ']),
            "BTC": float(row['BTC']),
            "NFLX": float(row['NFLX'])
        },
        "year": row['date'].year,
        "month": row['date'].month,
        "day": row['date'].day
    }
    for _, row in df.iterrows()
]
mongo_collection.insert_many(records)

print(f"MongoDB populated: {mongo_collection.count_documents({})} documents (one per date)")

# VERIFICATION QUERIES


print("\n" + "="*80)
print(" VERIFICATION - MySQL Database ")
print("="*80)

with mysql_engine.connect() as conn:
    # 1. Summary stats
    summary = pd.read_sql("""
        SELECT 
            COUNT(DISTINCT d.trading_date) AS unique_dates,
            COUNT(DISTINCT a.symbol) AS assets_count,
            COUNT(*) AS total_price_records
        FROM asset_prices p
        JOIN trading_dates d ON p.date_id = d.date_id
        JOIN assets a ON p.asset_id = a.asset_id
    """, conn)
    print("\nDatabase Summary:")
    print(summary.to_string(index=False))

    # 2. Latest 5 dates with all prices (pivoted)
    latest_prices = pd.read_sql("""
        SELECT 
            d.trading_date,
            MAX(CASE WHEN a.symbol = 'AMZN' THEN p.close_price END) AS AMZN,
            MAX(CASE WHEN a.symbol = 'DPZ' THEN p.close_price END) AS DPZ,
            MAX(CASE WHEN a.symbol = 'BTC' THEN p.close_price END) AS BTC,
            MAX(CASE WHEN a.symbol = 'NFLX' THEN p.close_price END) AS NFLX
        FROM asset_prices p
        JOIN trading_dates d ON p.date_id = d.date_id
        JOIN assets a ON p.asset_id = a.asset_id
        GROUP BY d.trading_date
        ORDER BY d.trading_date DESC
        LIMIT 5
    """, conn)
    print("\nLatest 5 trading days (all assets):")
    print(latest_prices.to_string(index=False))

    # 3. Average price per asset
    avg_prices = pd.read_sql("""
        SELECT a.symbol, ROUND(AVG(p.close_price), 2) AS avg_price
        FROM asset_prices p
        JOIN assets a ON p.asset_id = a.asset_id
        GROUP BY a.symbol
        ORDER BY avg_price DESC
    """, conn)
    print("\nAverage closing price per asset:")
    print(avg_prices.to_string(index=False))

print("\n" + "="*80)
print(" VERIFICATION - MongoDB Collection ")
print("="*80)

# 1. Total count
print(f"Total documents: {mongo_collection.count_documents({})}")

# 2. Most recent document
recent = mongo_collection.find_one(sort=[("date", -1)])
print("\nMost recent document:")
print(json.dumps(recent, indent=2, default=str))

# 3. Sample 3 documents from 2018 (as table-like)
print("\nSample documents from 2018:")
for doc in mongo_collection.find({"year": 2018}).limit(3).sort("date", 1):
    print(json.dumps(doc, indent=2, default=str))
    print("-" * 60)

print("\n" + "="*80)
print(" SETUP COMPLETE - Databases are ready for Task 2 queries & ERD ")
print("="*80)
