import pandas as pd
from sqlalchemy import create_engine

# 1. Load CSV
df = pd.read_csv("flipkart.csv")

# 2. KEEP ONLY REQUIRED COLUMNS (VERY IMPORTANT)
df = df[[
    "product_id",
    "product_name",
    "category",
    "brand",
    "seller_city",
    "price",
    "stock_available",
    "units_sold"
]]

# 3. OPTIONAL: remove duplicates
df = df.drop_duplicates(subset="product_id")

# 4. PostgreSQL connection
engine = create_engine(
    "postgresql+psycopg2://postgres:root@localhost:5432/Flipkart"
)

# 5. Insert data
df.to_sql(
    "products",
    engine,
    if_exists="append",
    index=False
)

print("Products data loaded successfully")
