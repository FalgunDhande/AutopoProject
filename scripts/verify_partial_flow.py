import sys
import os
import json
import time
import psycopg2
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.db_config import DB_CONFIG
from core.optimized_agent import process_po, handle_partial_response

def verify_partial_flow():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    ts = int(time.time())
    po_num = f"PO-PARTIAL-TEST-{ts}"
    print(f"--- 1. Creating PO {po_num} with Partial Stock ---")
    
    # We set FKP0000012 to 2 units. We will request 5.
    dummy_json = {
        "extracted_data": {
            "buyer": {
                "company_name": "Acme Globall Industries",
                "email": "acme@example.com", 
            }
        }
    }
    json_str = json.dumps(dummy_json)

    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier, total_amount, status, raw_json, sender_email
        ) VALUES (
            %s, '2026-01-30', 'Acme Globall Industries', 'Flipkart Wholesale', 10000, 'NEW', %s, 'involexis.team@gmail.com'
        ) RETURNING po_id
    """, (po_num, json_str))
    
    po_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO purchase_order_items (
            po_id, product_id, product_name, quantity, unit_price
        ) VALUES (
            %s, 'FKP0000012', 'Philips Series 768', 5, 2000
        )
    """, (po_id,))
    
    conn.commit()
    conn.close()
    
    print(f"PO {po_id} created. Running Agent...")
    process_po(po_id)
    
    print("\n--- 2. Verifying Status after Agent (Expect WAITING_FOR_REPLY) ---")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT status FROM purchase_orders WHERE po_id = %s", (po_id,))
    status = cur.fetchone()[0]
    print(f"Current Status: {status}")
    conn.close()

    print("\n--- 3. Simulating Approval (APPROVE) ---")
    handle_partial_response(po_num, "APPROVE")

    print("\n--- 4. Verifying Final Status (Expect PARTIAL_COMPLETED) ---")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT status FROM purchase_orders WHERE po_id = %s", (po_id,))
    status = cur.fetchone()[0]
    print(f"Final Status: {status}")
    conn.close()

if __name__ == "__main__":
    verify_partial_flow()
