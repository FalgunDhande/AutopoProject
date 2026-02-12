import sys
import os
from sqlalchemy import create_engine, text
from config.db_config import DB_CONFIG
import pandas as pd

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_engine():
    """Get SQLAlchemy engine for PostgreSQL."""
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(conn_str)

def get_po_summary():
    """Get summary statistics for purchase orders."""
    engine = get_engine()
    query = text("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed,
            COUNT(CASE WHEN status = 'PARTIAL_COMPLETED' THEN 1 END) as partial,
            COUNT(CASE WHEN status = 'WAITING_FOR_REPLY' THEN 1 END) as pending,
            COUNT(CASE WHEN status LIKE 'FAILED%' OR status = 'CANCELLED_BY_CUSTOMER' THEN 1 END) as failed
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
            total_amount, status, sender_email, created_at
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
        header_df = pd.read_sql(text("SELECT * FROM purchase_orders WHERE po_id = :id"), conn, params={"id": po_id})
        items_df = pd.read_sql(text("SELECT * FROM purchase_order_items WHERE po_id = :id"), conn, params={"id": po_id})
    return header_df, items_df

def get_monthly_sales():
    """Get top selling products for current month."""
    engine = get_engine()
    query = text("""
        SELECT 
            COALESCE(i.product_name, poi.product_name) as product_name,
            SUM(poi.quantity) as total_quantity,
            SUM(poi.line_total) as total_revenue
        FROM purchase_order_items poi
        JOIN purchase_orders po ON poi.po_id = po.po_id
        LEFT JOIN inventory i ON poi.product_id = i.product_id
        WHERE po.status IN ('COMPLETED', 'PARTIAL_COMPLETED')
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
    query = text("SELECT COUNT(*) as total_emails FROM purchase_orders WHERE sender_email IS NOT NULL")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def get_recent_activity(limit=10):
    """Get recent PO activity."""
    engine = get_engine()
    query = text("""
        SELECT po_number, buyer, status, total_amount, created_at
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
    query = text("SELECT product_id, product_name, stock_available, units_sold FROM inventory ORDER BY product_id")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


