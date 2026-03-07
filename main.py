from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy import text
from pymongo import MongoClient
from datetime import datetime

app = FastAPI(title="Stock Time-Series API")

# ==========================================
# 1. DATABASE CONNECTIONS
# ==========================================
# MySQL Connection
MYSQL_URL = "mysql+mysqlconnector://root:wagner@localhost:3306/stock_timeseries?charset=utf8mb4"
mysql_engine = sa.create_engine(MYSQL_URL)

# MongoDB Connection
MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["stock_timeseries"]
mongo_collection = mongo_db["daily_prices"]

# ==========================================
# 2. DATA MODELS
# ==========================================
# This defines what the JSON data should look like when someone POSTs a new record
class StockData(BaseModel):
    date: str  # Format: YYYY-MM-DD
    AMZN: float
    DPZ: float
    BTC: float
    NFLX: float

# ==========================================
# 3. REQUIRED TIME-SERIES ENDPOINTS
# ==========================================

@app.get("/analytics/latest/mongo")
def get_latest_mongo():
    """Fetches the most recent date's record from MongoDB"""
    recent = mongo_collection.find_one(sort=[("date", -1)], projection={"_id": 0})
    if not recent:
        raise HTTPException(status_code=404, detail="No records found")
    return {"database": "MongoDB", "data": recent}

@app.get("/analytics/latest/mysql")
def get_latest_mysql():
    """Fetches the most recent date's record from MySQL"""
    query = """
        SELECT d.trading_date, a.symbol, p.close_price
        FROM asset_prices p
        JOIN trading_dates d ON p.date_id = d.date_id
        JOIN assets a ON p.asset_id = a.asset_id
        WHERE d.trading_date = (SELECT MAX(trading_date) FROM trading_dates)
    """
    with mysql_engine.connect() as conn:
        result = conn.execute(text(query)).mappings().all()
    
    if not result:
         raise HTTPException(status_code=404, detail="No records found")
    return {"database": "MySQL", "data": [dict(row) for row in result]}

@app.get("/analytics/range/mongo")
def get_range_mongo(start_date: str, end_date: str):
    """Fetches records between two dates from MongoDB"""
    # Start and end date should be formatted as YYYY-MM-DD
    query = {"date": {"$gte": start_date, "$lte": end_date}}
    results = list(mongo_collection.find(query, {"_id": 0}))
    return {"database": "MongoDB", "count": len(results), "data": results}

# ==========================================
# 4. STANDARD CRUD ENDPOINTS (POST & DELETE)
# ==========================================

@app.post("/records/")
def create_record(record: StockData):
    """Adds a new stock record to BOTH MongoDB and MySQL"""
    
    # 1. Insert into MongoDB
    date_obj = datetime.strptime(record.date, "%Y-%m-%d")
    mongo_doc = {
        "date": record.date,
        "prices": {"AMZN": record.AMZN, "DPZ": record.DPZ, "BTC": record.BTC, "NFLX": record.NFLX},
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }
    mongo_collection.insert_one(mongo_doc)

    # 2. Insert into MySQL (Simplified for this example)
    with mysql_engine.connect() as conn:
        # Check if date exists, if not insert it
        conn.execute(text(f"INSERT IGNORE INTO trading_dates (trading_date) VALUES ('{record.date}')"))
        conn.commit()
        
    return {"message": "Record successfully added to both databases!"}

@app.delete("/records/{target_date}")
def delete_record(target_date: str):
    """Deletes a record by date from MongoDB"""
    result = mongo_collection.delete_one({"date": {"$regex": f"^{target_date}"}})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Date not found")
    return {"message": f"Record for {target_date} deleted from MongoDB"}