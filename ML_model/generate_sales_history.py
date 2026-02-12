import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine(
    "postgresql+psycopg2://postgres:root@localhost:5432/Flipkart"
)

products = pd.read_sql(
    "SELECT product_id, units_sold FROM products",
    engine
)

sales_rows = []
months = pd.date_range(end=datetime.today(), periods=12, freq="M")

for _, row in products.iterrows():
    total = row["units_sold"]
    monthly = np.random.dirichlet(np.ones(12)) * total

    for m, q in zip(months, monthly):
        sales_rows.append({
            "product_id": row["product_id"],
            "sale_date": m.date(),
            "quantity_sold": int(round(q))
        })

sales_df = pd.DataFrame(sales_rows)

sales_df.to_sql("sales_history", engine, if_exists="append", index=False)

print("Sales history created")
