import pandas as pd
from sqlalchemy import create_engine
import os
import time

time.sleep(15)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
)

print("📥 Loading data...")

df = pd.read_sql("SELECT * FROM sales", engine)
df.columns = df.columns.str.strip()

# Clean
df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')

# ------------------------
# QUARTERLY
# ------------------------
df['quarter'] = df['Order Date'].dt.to_period('Q')
quarterly = df.groupby('quarter')['Sales'].sum().reset_index()
quarterly['quarter'] = quarterly['quarter'].astype(str)

# ------------------------
# MONTHLY
# ------------------------
df['month'] = df['Order Date'].dt.to_period('M')
monthly = df.groupby('month')['Sales'].sum().reset_index()
monthly['month'] = monthly['month'].astype(str)

# ------------------------
# REGION
# ------------------------
region = df.groupby('Region')['Sales'].sum().reset_index()

# ------------------------
# CATEGORY
# ------------------------
category = df.groupby('Category')['Sales'].sum().reset_index()

print("🚀 Writing tables...")

quarterly.to_sql('sales_quarterly', engine, if_exists='replace', index=False)
monthly.to_sql('sales_monthly', engine, if_exists='replace', index=False)
region.to_sql('sales_by_region', engine, if_exists='replace', index=False)
category.to_sql('sales_by_category', engine, if_exists='replace', index=False)

print("✅ Processing complete")