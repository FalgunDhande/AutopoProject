import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg2
import json
import time
from config.db_config import DB_CONFIG
from core.optimized_agent import process_po

# TEST SCENARIO
# JSON says: "json_contact@example.com"
# ACTUAL SENDER: "involexis.team@gmail.com" (We want reply to go here)

JSON_EMAIL = "json_contact@example.com"
SENDER_EMAIL = "involexis.team@gmail.com"

def create_po_with_sender():
    ts = int(time.time())
    po_num = f"PO-SENDER-TEST-{ts}"
    print(f"Creating PO {po_num} with DISTINCT Sender vs JSON email...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    dummy_json = {
        "extracted_data": {
            "buyer": {
                "name": "Tech Corp Solutions",
                "email": JSON_EMAIL, 
            }
        }
    }
    json_str = json.dumps(dummy_json)

    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier, total_amount, status, raw_json, sender_email
        ) VALUES (
            %s, '2026-01-28', 'Tech Corp Solutions', 'Flipkart Wholesale', 5000, 'NEW', %s, %s
        ) RETURNING po_id
    """, (po_num, json_str, SENDER_EMAIL))
    
    po_id = cur.fetchone()[0]

    # Item
    cur.execute("""
        INSERT INTO purchase_order_items (
            po_id, product_id, product_name, quantity, unit_price
        ) VALUES (
            %s, 'FKP0000001', 'Test Product Full Stock', 5, 500
        )
    """, (po_id,))
    
    conn.commit()
    conn.close()
    
    return po_id

if __name__ == "__main__":
    po_id = create_po_with_sender()
    print(f"\n--- Running Agent for PO {po_id} ---")
    print(f"Expect email to be sent to: {SENDER_EMAIL} (NOT {JSON_EMAIL})")
    process_po(po_id)
