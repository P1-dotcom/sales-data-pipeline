from fastapi import FastAPI, Query
from sqlalchemy import create_engine
import os
import pandas as pd

app = FastAPI()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
)

@app.get("/")
def home():
    return {"message": "Sales Data API Running"}

# -------------------------
# QUARTERLY WITH FILTER
# -------------------------
@app.get("/data")
def get_data(year: str = None, quarter: str = None):
    df = pd.read_sql("SELECT * FROM sales_quarterly", engine)

    if year:
        df = df[df['quarter'].str.startswith(year)]

    if quarter:
        df = df[df['quarter'] == quarter]

    return df.to_dict(orient="records")

# -------------------------
# MONTHLY
# -------------------------
@app.get("/monthly")
def get_monthly():
    df = pd.read_sql("SELECT * FROM sales_monthly", engine)
    return df.to_dict(orient="records")

# -------------------------
# REGION
# -------------------------
@app.get("/region")
def get_region():
    df = pd.read_sql("SELECT * FROM sales_by_region", engine)
    return df.to_dict(orient="records")

# -------------------------
# CATEGORY
# -------------------------
@app.get("/category")
def get_category():
    df = pd.read_sql("SELECT * FROM sales_by_category", engine)
    return df.to_dict(orient="records")