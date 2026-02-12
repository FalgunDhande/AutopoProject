import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import calendar

from urllib.parse import quote_plus
from config.db_config import DB_CONFIG

encoded_password = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ""
url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

engine = create_engine(url)

# Load sales history
sales = pd.read_sql(
    "SELECT product_id, sale_date, quantity_sold FROM sales_history",
    engine
)

sales["month"] = pd.to_datetime(sales["sale_date"]).dt.month
rows = []

for product_id, g in sales.groupby("product_id"):

    if len(g) < 3:
        continue

    # -------- SEASONALITY (one-hot months) --------
    X = pd.get_dummies(g["month"], prefix="month")
    y = g["quantity_sold"]

    model = LinearRegression()
    model.fit(X, y)

    # Predict next month
    next_month = (g["month"].max() % 12) + 1
    next_X = pd.get_dummies(pd.Series([next_month]), prefix="month")
    next_X = next_X.reindex(columns=X.columns, fill_value=0)

    predicted_qty = int(max(0, model.predict(next_X)[0]))

    # -------- FIND PEAK (MOST REQUIRED) MONTH --------
    monthly_avg = g.groupby("month")["quantity_sold"].mean()
    peak_month_num = int(monthly_avg.idxmax())
    peak_month_name = calendar.month_name[peak_month_num]

    # -------- SEASONAL NOTE (human readable) --------
    seasonal_note = f"Mostly required in {peak_month_name}"

    rows.append({
        "product_id": product_id,
        "forecast_month": pd.Timestamp.today().replace(day=1),
        "predicted_quantity": predicted_qty,
        "peak_month": peak_month_name,
        "seasonal_note": seasonal_note
    })

forecast_df = pd.DataFrame(rows)

forecast_df.to_sql(
    "demand_forecast",
    engine,
    if_exists="append",
    index=False
)

print("✅ Seasonal demand forecasting with peak-month insight completed")
