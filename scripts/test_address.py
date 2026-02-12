
import psycopg2
import json
from db_config import DB_CONFIG
from optimized_agent import process_po

def create_dummy_po_with_address():
    print("Creating Dummy PO with Address...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create dummy JSON with address
    dummy_json = {
        "extracted_data": {
            "buyer": {
                "name": "Tech Corp Solutions",
                "address": "123 Innovation Drive, Silicon Valley, CA 94000",
                "gst_number": "GST-999-888"
            }
        }
    }
    json_str = json.dumps(dummy_json)

    # 1. Create a PO with product FKP0000001
    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier, total_amount, status, raw_json
        ) VALUES (
            'PO-ADDRESS-TEST', '2026-01-28', 'Tech Corp Solutions', 'Flipkart Wholesale', 5000, 'NEW', %s
        ) RETURNING po_id
    """, (json_str,))
    
    po_id = cur.fetchone()[0]

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
    po_id = create_dummy_po_with_address()
    print(f"\n--- Running Agent for PO {po_id} ---")
    process_po(po_id)
