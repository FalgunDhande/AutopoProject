import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg2
from config.db_config import DB_CONFIG
from core.optimized_agent import process_po

def create_dummy_po():
    print("Creating Dummy PO...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. Create a PO with product FKP0000001 (which we saw has 267 stock)
    # We will request 10 units -> Expect FULL Invoice
    
    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier, total_amount, status
        ) VALUES (
            'PO-TEST-001', '2026-01-28', 'Test Buyer Corp', 'Flipkart Wholesale', 5000, 'NEW'
        ) RETURNING po_id
    """)
    po_id_full = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO purchase_order_items (
            po_id, product_id, product_name, quantity, unit_price
        ) VALUES (
            %s, 'FKP0000001', 'Test Product Full Stock', 10, 500
        )
    """, (po_id_full,))
    
    conn.commit()
    print(f"Created PO {po_id_full} (Should be FULL stock)")
    
    # 2. Create a PO with product FKP0000002 (stock 16)
    # We request 100 units -> Expect PARTIAL Proposal
    
    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier, total_amount, status
        ) VALUES (
            'PO-TEST-002', '2026-01-28', 'Test Buyer Corp', 'Flipkart Wholesale', 10000, 'NEW'
        ) RETURNING po_id
    """)
    po_id_partial = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO purchase_order_items (
            po_id, product_id, product_name, quantity, unit_price
        ) VALUES (
            %s, 'FKP0000002', 'Test Product Partial Stock', 100, 100
        )
    """, (po_id_partial,))

    conn.commit()
    print(f"Created PO {po_id_partial} (Should be PARTIAL stock)")
    
    conn.close()
    
    return po_id_full, po_id_partial

if __name__ == "__main__":
    po1, po2 = create_dummy_po()
    
    print("\n--- Running Agent for PO 1 ---")
    process_po(po1)
    
    print("\n--- Running Agent for PO 2 ---")
    process_po(po2)
