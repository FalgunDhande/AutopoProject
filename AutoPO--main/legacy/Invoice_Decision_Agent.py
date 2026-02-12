import psycopg2
import requests
import json
from datetime import datetime

# Import invoice generator
from invoice_gen4 import generate_invoice_from_agent


# ============================================================
# 1. LOAD COMPANY DETAILS (FOR EMAIL SENDER INFO)
# ============================================================

with open("compnay_info.json", "r") as f:
    COMPANY = json.load(f)


# ============================================================
# 2. DATABASE CONNECTION
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": "Isha@2609"
}

def db_connect():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# 3. OLLAMA LLM CALL (Qwen 2.5)
# ============================================================

def llama(prompt):
    try:
        res = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "qwen2.5:7b",
                "prompt": prompt,
                "stream": False
            },
            timeout=50
        )
        res.raise_for_status()
        return res.json().get("response", "").strip()

    except Exception as e:
        print("🔥 LLM Error:", e)
        return "Error"


# ============================================================
# 4. POSTGRES QUERIES
# ============================================================

def get_po_header(po_id):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT po_id, po_number, buyer, supplier, status
        FROM purchase_orders
        WHERE po_id = %s
    """, (po_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "po_id": row[0],
        "po_number": row[1],
        "buyer": row[2],
        "supplier": row[3],
        "status": row[4]
    }


def get_po_items(po_id):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT item_id, product_id, product_name, quantity
        FROM purchase_order_items
        WHERE po_id = %s
    """, (po_id,))

    rows = cur.fetchall()
    conn.close()

    items = []
    for r in rows:
        items.append({
            "item_id": r[0],
            "product_id": r[1],
            "product_name": r[2],
            "requested_qty": r[3]
        })

    return items


def get_inventory(product_id):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT stock_available
        FROM inventory
        WHERE product_id = %s
    """, (product_id,))

    row = cur.fetchone()
    conn.close()

    return int(row[0]) if row else 0


def update_inventory(product_id, qty_change):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        UPDATE inventory
        SET stock_available = stock_available + %s
        WHERE product_id = %s
    """, (qty_change, product_id))

    conn.commit()
    conn.close()

    print(f"✔ Inventory updated for {product_id}: {qty_change}")


def update_po_status(po_id, status):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        UPDATE purchase_orders
        SET status = %s
        WHERE po_id = %s
    """, (status, po_id))

    conn.commit()
    conn.close()

    print(f"✔ PO {po_id} updated → {status}")


def log_event(action, message):
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO activity_log (action, message, timestamp)
        VALUES (%s, %s, %s)
    """, (action, message, datetime.now()))

    conn.commit()
    conn.close()


# ============================================================
# 5. CHECK INVENTORY STATUS
# ============================================================

def check_inventory(requested, available):
    if available >= requested:
        return "full"
    elif available == 0:
        return "none"
    return "partial"


# ============================================================
# 6. EMAIL GENERATORS (NOW USING REAL BUYER + COMPANY INFO)
# ============================================================

def send_mock_email(subject, body):
    print("\n=====================================")
    print("📧 SUBJECT:", subject)
    print(body)
    print("=====================================\n")


def email_full_invoice(po):
    buyer_name = po["buyer"]
    sender_name = COMPANY.get("contact_person", "Involexix Team")
    sender_email = COMPANY.get("email", "")
    company_name = COMPANY.get("name", "Involexix")

    prompt = f"""
Write a fully formatted professional email using EXACT values:

To: {buyer_name}
From: {sender_name} ({company_name})
Email: {sender_email}

Purpose:
Inform buyer that full stock for PO {po['po_number']} is available and the invoice is attached.

Rules:
- Use the real names provided above
- NO placeholders like [Recipient], [Your Name], etc.
- Keep it short, clean, professional.
"""

    return llama(prompt)


def email_partial_request(po, partial_items):
    buyer_name = po["buyer"]
    sender_name = COMPANY.get("contact_person", "Involexix Team")
    company_name = COMPANY.get("name", "Involexix")

    prompt = f"""
Write a short email to the client requesting approval for partial shipment.

To: {buyer_name}
From: {sender_name}, {company_name}

PO Number: {po['po_number']}

Rules:
- Do NOT use placeholders
- Write actual names
- Keep the message polite and concise
"""

    return llama(prompt)


def email_production_request(po):
    sender_name = COMPANY.get("contact_person", "Involexix Team")
    company_name = COMPANY.get("name", "Involexix")

    prompt = f"""
Write an internal email to Production Team.

Sender: {sender_name}, {company_name}
PO Number: {po['po_number']}

Purpose: Notify that items must be manufactured.

Rules:
- No placeholders
- Professional, clear
"""

    return llama(prompt)


# ============================================================
# 7. CLASSIFY CLIENT REPLY
# ============================================================

def analyze_client_reply(reply):
    prompt = f"""
Classify client's reply strictly as one of:

approve
reject
clarify

approve → "send what you have", "ship partial", "whatever is available"
reject → "send only full", "no partial"
clarify → "ok", "noted", "will check", unclear replies

Client reply: "{reply}"

Answer only the word: approve / reject / clarify
"""
    return llama(prompt).strip().lower()


# ============================================================
# 8. MAIN AGENT WORKFLOW
# ============================================================

def process_po(po_id):
    print(f"\n🔥 Processing PO {po_id}")

    po = get_po_header(po_id)
    if not po:
        print("❌ PO not found")
        return

    items = get_po_items(po_id)

    decisions = []

    # -------------------------
    # CHECK INVENTORY ITEM-WISE
    # -------------------------
    for item in items:
        requested = item["requested_qty"]
        available = get_inventory(item["product_id"])
        status = check_inventory(requested, available)

        decisions.append({
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "requested": requested,
            "available": available,
            "decision": status,
            "allocatable": min(requested, available)
        })

    print("\n📌 Decision Summary:")
    print(json.dumps(decisions, indent=2))


    # ----------------------------------------------------------
    # FULL STOCK → Generate invoice immediately
    # ----------------------------------------------------------
    if all(d["decision"] == "full" for d in decisions):

        send_mock_email("Full Invoice Ready", email_full_invoice(po))

        for d in decisions:
            update_inventory(d["product_id"], -d["requested"])

        invoice_path = generate_invoice_from_agent(po, decisions)
        print("📄 FULL INVOICE GENERATED:", invoice_path)

        update_po_status(po_id, "invoiced")
        log_event("invoice_full", f"PO {po_id} fully invoiced")

        return


    # ----------------------------------------------------------
    # NO STOCK → Send to production
    # ----------------------------------------------------------
    if all(d["decision"] == "none" for d in decisions):

        send_mock_email("Production Required", email_production_request(po))

        update_po_status(po_id, "production_needed")
        log_event("prod_full", f"PO {po_id} needs full production")

        return


    # ----------------------------------------------------------
    # PARTIAL STOCK → Request client approval
    # ----------------------------------------------------------
    partial_items = [d for d in decisions if d["decision"] != "full"]

    send_mock_email("Partial Approval Needed",
                    email_partial_request(po, partial_items))

    reply = input("📨 Client reply: ")
    decision = analyze_client_reply(reply)

    print("🧠 Client classification →", decision)


    # ----------------------------------------------------------
    # CLIENT APPROVES PARTIAL
    # ----------------------------------------------------------
    if decision == "approve":

        alloc_items = []
        for d in decisions:
            if d["allocatable"] > 0:
                update_inventory(d["product_id"], -d["allocatable"])
                alloc_items.append(d)

        invoice_path = generate_invoice_from_agent(po, alloc_items)
        print("📄 PARTIAL INVOICE GENERATED:", invoice_path)

        send_mock_email("Partial Invoice Ready", f"Invoice: {invoice_path}")

        update_po_status(po_id, "partial_approved")
        log_event("partial_yes", f"PO {po_id} partial approved")

        return


    # ----------------------------------------------------------
    # CLIENT REJECTS PARTIAL
    # ----------------------------------------------------------
    if decision == "reject":

        send_mock_email("Production Required", email_production_request(po))

        update_po_status(po_id, "production_required")
        log_event("partial_no", f"PO {po_id} full production required")

        return


    # ----------------------------------------------------------
    # CLIENT REPLY UNCLEAR → Ask again
    # ----------------------------------------------------------
    clarification_email = llama(
        f"Write a polite email requesting clarity on client's message: '{reply}'"
    )

    send_mock_email("Need Clarification", clarification_email)

    update_po_status(po_id, "clarification_required")
    log_event("clarify", f"PO {po_id}: clarification requested")


# ============================================================
# 9. RUN
# ============================================================

if __name__ == "__main__":
    process_po(5)
