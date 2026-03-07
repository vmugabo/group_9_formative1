import requests
import pandas as pd
import numpy as np
import joblib
from tensorflow.keras.models import load_model

print("1. Fetching time-series records from API...")
# We use your range endpoint to grab enough historical data to cover the 30-day requirement
API_URL = "http://127.0.0.1:8000/analytics/range/mongo?start_date=2019-03-01&end_date=2019-05-14"
response = requests.get(API_URL)
data = response.json()["data"]

# Clean up the JSON data into a DataFrame
df = pd.DataFrame([item["prices"] for item in data])

# The model strictly expects the columns in this exact order:
features = ['AMZN', 'DPZ', 'NFLX', 'BTC']
df = df[features]

# Isolate just the last 30 days to make our prediction
df_last_30 = df.tail(30)
if len(df_last_30) < 30:
    print("Error: The API didn't return enough days. The LSTM needs exactly 30.")
    exit()

print("2. Preprocessing data (scaling)...")
# Load the exact scalers your teammate used so the math matches
scaler_X = joblib.load("scaler_X2.pkl")
scaler_y = joblib.load("scaler_y2.pkl")

# Scale the numbers and reshape them into the 3D block the LSTM requires: (1 sample, 30 days, 4 features)
X_scaled = scaler_X.transform(df_last_30)
X_input = np.reshape(X_scaled, (1, 30, 4))

print("3. Loading the trained LSTM model...")
model = load_model("lstm_model.keras")

print("4. Generating forecast...")
# The model spits out a scaled number (e.g., 0.85), so we reverse the scaling to get the real dollar amount
prediction_scaled = model.predict(X_input, verbose=0)
prediction_real = scaler_y.inverse_transform(prediction_scaled)

print("\n" + "="*45)
print(f"💰 FORECASTED NEXT-DAY BTC PRICE: ${prediction_real[0][0]:,.2f}")
print("="*45)