
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from config.db_config import DB_CONFIG
from urllib.parse import quote_plus
import sys

def get_engine():
    encoded_password = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ""
    # Handle empty password
    if encoded_password:
        url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    else:
        url = f"postgresql+psycopg2://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(url)

def update_history():
    print("🔄 Updating Sales History Model...")
    engine = get_engine()

    # 1. Fetch current inventory + historical sales volume
    print("   Fetching product data...")
    try:
        products = pd.read_sql(
            "SELECT product_id, units_sold FROM inventory",
            engine
        )
    except Exception as e:
        print(f"❌ Failed to read inventory: {e}")
        return

    if products.empty:
        print("❌ no products found.")
        return

    # 2. Generate Synthetic "History" based on 'units_sold'
    # We distribute the total 'units_sold' randomly over the last 12 months
    sales_rows = []
    
    # Generate past 12 months dates
    start_date = datetime.today().replace(day=1)
    months = [start_date - timedelta(days=30*i) for i in range(12)]
    months.reverse() # Oldest to newest
    
    print(f"   Generating history for {len(products)} products...")
    
    for _, row in products.iterrows():
        total = row["units_sold"]
        if total <= 0: continue
        
        # Dirichlet distribution for randomness summing to 1
        monthly_dist = np.random.dirichlet(np.ones(12))
        monthly_sales = np.round(monthly_dist * total).astype(int)
        
        # Ensure sum matches total (simple adjustment)
        diff = total - monthly_sales.sum()
        monthly_sales[-1] += diff 

        for m, q in zip(months, monthly_sales):
            if q > 0:
                sales_rows.append({
                    "product_id": row["product_id"],
                    "sale_date": m.date(),
                    "quantity_sold": int(q)
                })

    sales_df = pd.DataFrame(sales_rows)
    
    if sales_df.empty:
        print("⚠️ No sales history generated (units_sold might be 0 everywhere).")
        return

    # 3. Save to DB
    print(f"   Saving {len(sales_df)} sales records...")
    sales_df.to_sql("sales_history", engine, if_exists="replace", index=False)
    
    print("✅ Sales History Updated.")

if __name__ == "__main__":
    update_history()
