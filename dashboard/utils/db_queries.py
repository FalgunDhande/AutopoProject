import sys
import os
from sqlalchemy import create_engine, text
from config.db_config import DB_CONFIG
import pandas as pd

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_engine():
    """Get SQLAlchemy engine for PostgreSQL."""
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['dbname']}"
    )
    return create_engine(conn_str)

# -------------------- DASHBOARD QUERIES --------------------

def get_po_summary():
    """Get summary statistics for purchase orders."""
    engine = get_engine()
    query = text("""
        SELECT 
            COUNT(*) AS total,
            COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed,
            COUNT(CASE WHEN order_status = 'PARTIAL_COMPLETED' THEN 1 END) AS partial,
            COUNT(CASE WHEN order_status = 'WAITING_FOR_REPLY' THEN 1 END) AS pending,
            COUNT(CASE 
                WHEN order_status LIKE 'FAILED%' 
                  OR order_status = 'CANCELLED_BY_CUSTOMER'
                THEN 1 
            END) AS failed
        FROM purchase_orders
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def get_all_pos():
    """Get all purchase orders with details."""
    engine = get_engine()
    query = text("""
        SELECT 
            po_id, po_number, po_date, buyer, supplier, 
            total_amount, order_status, sender_email, created_at
        FROM purchase_orders
        ORDER BY created_at DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def get_po_details(po_id):
    """Get detailed information for a specific PO including line items."""
    engine = get_engine()
    with engine.connect() as conn:
        header_df = pd.read_sql(
            text("SELECT * FROM purchase_orders WHERE po_id = :id"),
            conn,
            params={"id": po_id}
        )
        items_df = pd.read_sql(
            text("SELECT * FROM purchase_order_items WHERE po_id = :id"),
            conn,
            params={"id": po_id}
        )
    return header_df, items_df

def get_monthly_sales():
    """Get top selling products."""
    engine = get_engine()
    query = text("""
        SELECT 
            COALESCE(i.product_name, poi.product_name) AS product_name,
            SUM(poi.quantity) AS total_quantity,
            SUM(poi.line_total) AS total_revenue
        FROM purchase_order_items poi
        JOIN purchase_orders po ON poi.po_id = po.po_id
        LEFT JOIN inventory i ON poi.product_id = i.product_id
        WHERE po.order_status IN ('COMPLETED', 'PARTIAL_COMPLETED')
        GROUP BY 1
        ORDER BY total_quantity DESC
        LIMIT 10
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def get_email_count():
    """Get count of emails processed."""
    engine = get_engine()
    query = text("""
        SELECT COUNT(*) AS total_emails
        FROM purchase_orders
        WHERE sender_email IS NOT NULL
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def get_recent_activity(limit=10):
    """Get recent PO activity."""
    engine = get_engine()
    query = text("""
        SELECT 
            po_number, buyer, order_status, 
            total_amount, created_at
        FROM purchase_orders
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"limit": limit})
    return df

def get_inventory_status():
    """Get current inventory status."""
    engine = get_engine()
    query = text("""
        SELECT 
            product_id, product_name, 
            stock_available, units_sold
        FROM inventory
        ORDER BY product_id
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df
