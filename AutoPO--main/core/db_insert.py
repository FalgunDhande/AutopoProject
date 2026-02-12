import json
import psycopg2
from config.db_config import DB_CONFIG


def insert_po(final_json, sender_email=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    d = final_json["extracted_data"]

    buyer = d.get("buyer", {})
    seller = d.get("seller", {})

    # -------- EXTRACT SENDER EMAIL --------
    if not sender_email:
        raw_from = final_json.get("email_metadata", {}).get("from_email", "")
        if "<" in raw_from and ">" in raw_from:
            sender_email = raw_from.split("<")[1].split(">")[0]
        else:
            sender_email = raw_from
            
    print(f"📧 Sender Email for DB: {sender_email}")

    # -------- INSERT PO HEADER --------
    cur.execute("""
        INSERT INTO purchase_orders (
            po_number, po_date, buyer, supplier,
            buyer_gst, supplier_gst, currency,
            total_amount, raw_json, sender_email
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING po_id
    """, (
        d.get("po_number"),
        d.get("po_date") or None,
        buyer.get("company_name"),
        seller.get("company_name"),
        buyer.get("gst_number"),
        seller.get("gst_number"),
        d.get("currency"),
        d.get("total_amount"),
        json.dumps(final_json),    # FULL JSON stored safely
        sender_email               # FROM EMAIL (Ingestion)
    ))

    po_id = cur.fetchone()[0]

    # -------- INSERT LINE ITEMS --------
    for item in d.get("line_items", []):
        cur.execute("""
            INSERT INTO purchase_order_items (
                po_id, product_id, product_name,
                quantity, unit_price, line_total
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            po_id,
            item.get("product_id"),
            item.get("description"),
            _safe_int(item.get("quantity")),
            _safe_numeric(item.get("unit_price")),
            _safe_numeric(item.get("line_total"))
        ))

    conn.commit()
    cur.close()
    conn.close()

    # -------- TRIGGER AGENT --------
    try:
        from core.optimized_agent import process_po
        process_po(po_id)
    except Exception as e:
        print(f"Agent failed to process PO {po_id}: {e}")


def _safe_numeric(val):
    if not val:
        return None
    try:
        # Remove commas, currency symbols, and any whitespace
        clean_val = str(val).replace(",", "").replace("INR", "").replace("$", "").strip()
        return float(clean_val)
    except:
        return None


def _safe_int(val):
    if not val:
        return None
    try:
        return int(float(val))
    except:
        return None