import pandas as pd
from sqlalchemy import create_engine
import os
import time

# Wait for DB to be ready
time.sleep(10)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
)

print("📥 Loading CSV...")

# Load dataset
df = pd.read_csv("/data/sales_data.csv")

# Clean column names
df.columns = df.columns.str.strip()

print("📊 Columns detected:", df.columns.tolist())

# Convert date columns safely
if 'Order Date' in df.columns:
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
else:
    raise Exception("❌ 'Order Date' column not found")

if 'Ship Date' in df.columns:
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce')
else:
    raise Exception("❌ 'Ship Date' column not found")

# Optional: Clean numeric fields
numeric_cols = ['Sales', 'Quantity', 'Profit']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

print("🚀 Writing to PostgreSQL...")

# Insert into DB
df.to_sql('sales', engine, if_exists='replace', index=False)

print("✅ Data ingestion completed successfully")