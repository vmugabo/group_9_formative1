# Time Series Stock Prices: MySQL + MongoDB + Forecasting

Group assignment on time-series data processing, dual-database implementation (MySQL & MongoDB), CRUD operations, and forecasting.
[PDF Report](https://docs.google.com/document/d/1NCNulXtb6SD3HT7VWqyT5OROOTRuj-bk-dun497EGEs/edit?usp=sharing)

**Dataset**  
Daily adjusted closing prices for AMZN, DPZ, BTC, NFLX (2013-05-01 to 2019-05-14, 1520 rows)  
Source: Kaggle stock prices dataset

## Features

- **Task 1**: EDA, analytical questions (incl. lags & moving averages), model training & comparison
- **Task 2**: 
  - MySQL: 3 normalized tables (`assets`, `trading_dates`, `asset_prices`) + ERD
  - MongoDB: Denormalized `daily_prices` collection (1 doc per day)
  - Setup, population, and verification queries
- **Task 3**: CRUD operations (direct Python calls) + required endpoints:
  - Latest record
  - Records by date range
  - For both MySQL and MongoDB
- **Task 4**: Prediction script (fetch from DB → preprocess → load model → forecast)

