import streamlit as st
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.theme import apply_theme, display_logo
import psycopg2
from config.db_config import DB_CONFIG
import pandas as pd

# Page config
st.set_page_config(
    page_title="Inventory - Involexis",
    page_icon="📦",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

# Increase table render limit for large datasets
pd.set_option("styler.render.max_elements", 1000000)

st.title("📦 Inventory Management")

def get_inventory():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Show all products
        query = "SELECT product_id, product_name, stock_available, units_sold FROM inventory ORDER BY product_id"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

try:
    inventory_df = get_inventory()
    
    if not inventory_df.empty:
        # Metrics row
        st.subheader("📊 Stock Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Items", len(inventory_df))
        with col2:
            st.metric("Low Stock", len(inventory_df[inventory_df['stock_available'] < 100]))
        with col3:
            st.metric("Total Units Sold", f"{inventory_df['units_sold'].sum():,}")
            
        st.markdown("---")
        
        # Search and Filter
        search = st.text_input("Search Product Name or ID")
        if search:
            inventory_df = inventory_df[
                (inventory_df['product_name'].str.contains(search, case=False, na=False)) |
                (inventory_df['product_id'].str.contains(search, case=False, na=False))
            ]
            
        # Inventory Table
        st.subheader("Stock Levels")
        
        # Color coding for stocks (Modern Light Mode)
        def color_stock(val):
            if val == 0: return 'background-color: #F8D7DA; color: #842029; font-weight: bold; border-radius: 4px;'
            elif val < 100: return 'background-color: #FFF3CD; color: #664D03; font-weight: bold; border-radius: 4px;'
            return 'color: #1B4965'

        st.dataframe(
            inventory_df.style.map(color_stock, subset=['stock_available']),
            use_container_width=True,
            hide_index=True
        )
        
    else:
        st.info("Inventory data is empty.")

except Exception as e:
    st.error(f"Error loading inventory: {str(e)}")
