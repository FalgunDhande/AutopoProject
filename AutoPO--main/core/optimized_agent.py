
import json
import psycopg2
from datetime import datetime
from config.db_config import DB_CONFIG
from core.invoice_generator import generate_invoice_for_po
import requests
import os
from dotenv import load_dotenv

# Load local environment variables
load_dotenv()

# ============================================================
# CONFIG
# ============================================================

# Mock Email (Print to console) or Real SMTP can be swapped here
ENABLE_EMAIL = True 
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:1.5b")

EMAIL_SIGNATURE = """
Involexis
Sales Team
Phone: +91-8924506823
Email: involexis.team@gmail.com
"""

# ============================================================
# DB HELPERS
# ============================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_po_id_by_number(po_number):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT po_id FROM purchase_orders WHERE po_number = %s", (po_number,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def get_po_details(po_id):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Fetch Header
    # Fetch Header and Raw JSON
    cur.execute("""
        SELECT po_number, buyer, supplier, total_amount, raw_json, sender_email
        FROM purchase_orders WHERE po_id = %s
    """, (po_id,))
    header = cur.fetchone()
    
    if not header:
        conn.close()
        return None, None

    # Parse raw_json for extra details like address
    raw_data = {}
    if header[4]:
        try:
            raw_data = json.loads(header[4])
        except:
            pass
            
    buyer_address = ""
    buyer_email_from_json = ""
    if raw_data:
        # extracted_data -> buyer -> address
        buyer_address = raw_data.get("extracted_data", {}).get("buyer", {}).get("address", "")
        buyer_email_from_json = raw_data.get("extracted_data", {}).get("buyer", {}).get("email", "")

    # PRIORITY: Sender Email (Ingestion) > JSON Email
    final_email = header[5] if header[5] else buyer_email_from_json

    po_header = {
        "po_number": header[0],
        "buyer": header[1],
        "supplier": header[2],
        "total": header[3],
        "buyer_address": buyer_address,
        "buyer_email": final_email 
    }

    # Fetch Items
    cur.execute("""
        SELECT product_id, product_name, quantity, unit_price 
        FROM purchase_order_items WHERE po_id = %s
    """, (po_id,))
    rows = cur.fetchall()
    
    items = []
    for r in rows:
        items.append({
            "product_id": r[0],
            "product_name": r[1],
            "requested": int(r[2]),
            "unit_price": float(r[3] or 0)
        })
    
    conn.close()
    return po_header, items

def get_inventory_batch(product_ids):
    """
    Fetch stock for multiple products in one query.
    Returns dict: {product_id: stock_available}
    """
    if not product_ids:
        return {}
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    placeholders = ",".join(["%s"] * len(product_ids))
    cur.execute(f"SELECT product_id, stock_available FROM inventory WHERE product_id IN ({placeholders})", tuple(product_ids))
    
    stock_map = {row[0]: row[1] for row in cur.fetchall()}
    conn.close()
    return stock_map

def update_inventory_stock(allocations):
    """
    Deduct stock for allocated items.
    allocations = [{"product_id": "X", "allocatable": 5}, ...]
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    for item in allocations:
        qty = item["allocatable"]
        if qty > 0:
            cur.execute("""
                UPDATE inventory SET stock_available = stock_available - %s 
                WHERE product_id = %s
            """, (qty, item["product_id"]))
            
    conn.commit()
    conn.close()

def update_po_status(po_id, status, notes=""):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE purchase_orders SET status = %s WHERE po_id = %s", (status, po_id))
    # Optionally log notes to a separate log table
    conn.commit()
    conn.close()


def reconstruct_decisions(items, stock_map):
    decisions = []
    for item in items:
        pid = item["product_id"]
        req = item["requested"]
        avail = stock_map.get(pid, 0)
        
        alloc = 0
        status = "NONE"
        
        if avail >= req:
            alloc = req
            status = "FULL"
        elif avail > 0:
            alloc = avail
            status = "PARTIAL"
        
        decisions.append({
            "product_id": pid,
            "product_name": item["product_name"],
            "requested": req,
            "available": avail,
            "allocatable": alloc,
            "status": status,
            "unit_price": item["unit_price"]
        })
    return decisions

# ============================================================
# LLM HELPER (for Email Body)
# ============================================================

def generate_email_body(prompt):
    full_prompt = f"{prompt}\n\nIMPORTANT: Do NOT include any signature or closing like 'Best regards', '[Your Name]', etc. Just write the body of the email. I will add the signature automatically."
    try:
        res = requests.post(OLLAMA_URL, json={
            "model": LLM_MODEL,
            "prompt": full_prompt,
            "stream": False
        }, timeout=30)
        body = res.json().get("response", "").strip()
        
        # 1. Replace Placeholders with Real Info
        replacements = {
            "[Your Contact Information]": "involexis.team@gmail.com",
            "[Your Name]": "Involexis Sales Team",
            "[Company Name]": "Involexis",
            "[Date]": datetime.now().strftime("%Y-%m-%d"),
            "[Supplier Name]": "Involexis"
        }
        for key, val in replacements.items():
            if key in body:
                body = body.replace(key, val)
        
        # 2. Post-processing: Remove common closers/signatures
        closers = ["Best regards", "Warm regards", "Sincerely", "Kind regards", "Regards", "Supplier:"]
        for closer in closers:
            # Case-insensitive check might be better, but exact match first
            if closer in body:
                body = body.split(closer)[0].strip()
            if closer.lower() in body.lower():
                 # Find index case-insensitively
                 idx = body.lower().rfind(closer.lower())
                 if idx != -1:
                     body = body[:idx].strip()
        
        # Remove trailing comma or dot if split weirdly
        if body.endswith(",") or body.endswith("."):
             pass # keeping dot is fine, comma is weird for signature split
        if body.endswith(","):
             body = body[:-1].strip()
             
        return body
    except:
        return "Please find the attached invoice."

# ============================================================
# SMTP EMAIL SENDER
# ============================================================

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

def send_email(to_email, subject, body, attachment_path=None):
    if not to_email:
        print("❌ Cannot send email: No recipient address found.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = to_email
        msg['Subject'] = subject

        full_body = f"{body}\n\n{EMAIL_SIGNATURE}"
        msg.attach(MIMEText(full_body, 'plain'))

        if attachment_path:
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f"attachment; filename= {filename}",
                )
                msg.attach(part)

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        text = msg.as_string()
        server.sendmail(EMAIL_USER, to_email, text)
        server.quit()
        
        print(f"📧 Sending Email with Subject: '{subject}'")
        print(f"✅ Email sent successfully to {to_email}")

    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# ============================================================
# REPLY HANDLER (CALLED BY LISTENER)
# ============================================================

def handle_partial_response(po_id_or_num, decision):
    """
    decision: 'APPROVE' or 'REJECT'
    """
    if isinstance(po_id_or_num, str) and not po_id_or_num.isdigit():
        po_id = get_po_id_by_number(po_id_or_num)
    else:
        po_id = int(po_id_or_num)

    if not po_id:
        print(f"❌ PO ID not found for: {po_id_or_num}")
        return

    print(f"🔄 Handling Partial Response for PO {po_id}: {decision}")
    
    if decision == "REJECT":
        update_po_status(po_id, "CANCELLED_BY_CUSTOMER")
        print("❌ PO Cancelled by customer request.")
        return

    if decision == "APPROVE":
        # 1. Re-check inventory (stock might have changed!)
        header, items = get_po_details(po_id)
        product_ids = [i["product_id"] for i in items]
        stock_map = get_inventory_batch(product_ids)
        
        decisions = reconstruct_decisions(items, stock_map)
        
        # 2. Allocate what is available
        available_items = [d for d in decisions if d["allocatable"] > 0]
        
        if not available_items:
            print("❌ Stock ran out while waiting for reply!")
            update_po_status(po_id, "FAILED_NO_STOCK")
            return

        # 3. Deduct Stock
        update_inventory_stock(available_items)
        
        # 4. Generate Invoice (now treated as Final)
        pdf_path = generate_invoice_for_po(po_id, header, available_items)
        print(f"📄 Partial Invoice Generated: {pdf_path}")
        
        body = generate_email_body(f"Write a thank you email to {header['buyer']} confirming partial shipment for PO {header['po_number']}.")
        body = generate_email_body(f"Write a thank you email to {header['buyer']} confirming partial shipment for PO {header['po_number']}.")
        send_email(header.get("buyer_email"), f"Confirmed: Partial Shipment for PO {header['po_number']}", body, pdf_path)
        
        update_po_status(po_id, "PARTIAL_COMPLETED")

# ============================================================
# CORE AGENT LOGIC
# ============================================================

def process_po(po_id):
    print(f"🤖 Agent Processing PO: {po_id}")
    
    header, items = get_po_details(po_id)
    if not header:
        print("❌ PO Not Found")
        return

    # 1. Check Inventory
    product_ids = [i["product_id"] for i in items]
    stock_map = get_inventory_batch(product_ids)
    
    decisions = reconstruct_decisions(items, stock_map)

    # 2. Analyze Outcome
    all_full = all(d["status"] == "FULL" for d in decisions)
    all_none = all(d["status"] == "NONE" for d in decisions)
    # has_partial = any(d["status"] == "PARTIAL" for d in decisions) or (not all_full and not all_none)

    # 3. Take Action
    
    if all_full:
        print("✅ Full Stock Available. Generating Invoice...")
        
        # Deduct Stock
        update_inventory_stock(decisions)
        
        # Generate Invoice
        pdf_path = generate_invoice_for_po(po_id, header, decisions)
        print(f"📄 Invoice Generated: {pdf_path}")
        
        # Send Email
        # Send Email
        subject = f"Invoice Submission – {header['po_number']}"
        body = f"""Dear {header['buyer']},

I hope this email finds you well.

Please find attached the fully generated invoice {header['po_number']} for the services/products provided as discussed. Kindly review the invoice and let me know if any additional information or clarification is required.

Thank you for your cooperation. I look forward to your confirmation.

Warm regards,"""
        
        send_email(header.get("buyer_email"), subject, body, pdf_path)
        
        update_po_status(po_id, "COMPLETED")

    elif all_none:
        print("❌ No Stock Available. Sending Apology.")
        
        prompt = f"""
        Act as the Supplier (Involexis). Write an email body to the Buyer ({header['buyer']}) regarding Purchase Order {header['po_number']}.
        
        Context:
        - All requested items are out of stock.
        - We will notify them when available.
        
        Instructions:
        1. Start directly with "Dear {header['buyer']},"
        2. Apologize for the stockout.
        3. Do NOT include a Subject line.
        4. Do NOT include a closing or signature.
        """
        body = generate_email_body(prompt)
        
        send_email(header.get("buyer_email"), f"Update on PO {header['po_number']}", body)
        
        update_po_status(po_id, "FAILED_NO_STOCK")

    else:
        # Partition Case (Partial or Mixed Full/None)
        print("⚠️ Partial Stock Available. Sending Proposal.")
        
        # Filter only items we CAN supply
        available_items = [d for d in decisions if d["allocatable"] > 0]
        
        if not available_items:
             # Should be covered by all_none, but safe fallback
             update_po_status(po_id, "FAILED_NO_STOCK")
             return

        # NEW FLOW: Don't generate invoice yet. Send email listing available items.
        item_list = "\n".join([f"- {d['product_name']}: {d['allocatable']} units" for d in available_items])
        
        prompt = f"""
        Act as the Supplier (Involexis). Write an email body to the Buyer ({header['buyer']}) regarding Purchase Order {header['po_number']}.
        
        Context:
        - We only have partial stock available.
        - Available Items:
        {item_list}
        
        Instructions:
        1. Start directly with "Dear {header['buyer']},"
        2. Inform them about the available items.
        3. Ask if they want us to ship these available items immediately while waiting for the rest.
        4. Do NOT include a Subject line.
        5. Do NOT include a closing or signature (I will add it automatically).
        6. Keep it professional and concise.
        """
        
        body = generate_email_body(prompt)
        send_email(header.get("buyer_email"), f"Update: Partial Stock for PO {header['po_number']}", body)
        
        update_po_status(po_id, "WAITING_FOR_REPLY")


if __name__ == "__main__":
    # Test manually
    import sys
    if len(sys.argv) > 1:
        process_po(int(sys.argv[1]))
