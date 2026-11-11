import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

url = URL.create(
    "mysql+pymysql",
    username=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DATABASE"),
)
engine = create_engine(url)

tables = {
    "customers": ("data/raw/customers.csv", {"customer_id": "Int64"}),
    "products": ("data/raw/products.csv", {"product_id": "Int64"}),
    "stg_orders": (
        "data/raw/orders.csv",
        {"order_id": "Int64", "customer_id": "Int64",
         "product_id": "Int64", "order_date": "string"},
    ),
}

with engine.begin() as conn:
    for table, (path, dtypes) in tables.items():
        df = pd.read_csv(path, dtype=dtypes)
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        df.to_sql(table, conn, if_exists="append", index=False)
        print(f"{table}: {len(df)} filas cargadas")