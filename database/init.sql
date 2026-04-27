CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    date DATE,
    sales_amount FLOAT
);

CREATE TABLE IF NOT EXISTS sales_aggregated (
    quarter TEXT,
    total_sales FLOAT
);