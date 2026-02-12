import os
import sys
import time
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.db_config import DB_CONFIG
import psycopg2

# ================= CONFIG ================= #
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "involexis.team@gmail.com"
EMAIL_PASS = "jspk yczo ykes ctji"
RECIPIENT = "involexis.team@gmail.com"

TEST_PDF = "test_assets/test_po_999.pdf"
PO_NUMBER = "PO-VERIFY-999"
# ========================================== #

def send_test_email():
    print(f"📧 Sending test email to {RECIPIENT}...")
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = RECIPIENT
    msg['Subject'] = f"New Purchase Order: {PO_NUMBER}"
    
    body = "Please process the attached Purchase Order."
    msg.attach(MIMEText(body, 'plain'))
    
    with open(TEST_PDF, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(TEST_PDF)}")
        msg.attach(part)
        
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)
    server.sendmail(EMAIL_USER, RECIPIENT, msg.as_string())
    server.quit()
    print("✅ Test email sent.")

def check_db_for_po():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT po_id, status, total_amount FROM purchase_orders WHERE po_number = %s", (PO_NUMBER,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Exception as e:
        print(f"DB Check Error: {e}")
        return None

def check_manifest():
    manifest_path = "manifest.json"
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path, "r") as f:
            data = json.load(f)
            # Find the entry that corresponds to our test file
            for fname, meta in data.items():
                if meta.get("email_metadata", {}).get("subject") == f"New Purchase Order: {PO_NUMBER}" or \
                   meta.get("email_metadata", {}).get("from_email") == EMAIL_USER:
                    return meta
    except:
        pass
    return None

def run_test():
    send_test_email()
    
    print("\n⏳ Waiting for pipeline to process (Est. 30-60s for OCR)...")
    start_time = time.time()
    timeout = 180 # 3 minutes
    
    stages = {
        "ingestion": False,
        "ocr": False,
        "db": False,
        "invoice": False
    }
    
    while time.time() - start_time < timeout:
        # Check Stage 1: Ingestion (via manifest or incoming folder)
        if not stages["ingestion"]:
            manifest = check_manifest()
            if manifest:
                print("🔹 [STAGE 1] Ingestion: SUCCESS (Found in manifest)")
                stages["ingestion"] = True
                
        # Check Stage 2 & 3: OCR and DB
        if not stages["db"]:
            db_row = check_db_for_po()
            if db_row:
                print(f"🔹 [STAGE 2/3] OCR & DB: SUCCESS (PO ID: {db_row[0]}, Status: {db_row[1]})")
                stages["ocr"] = True
                stages["db"] = True
                
        # Check Stage 4: Invoice
        if stages["db"] and not stages["invoice"]:
            # Check invoices folder
            invoices = os.listdir("invoices")
            for inv in invoices:
                if PO_NUMBER in inv:
                    print(f"🔹 [STAGE 4] Invoice: SUCCESS (Found {inv})")
                    stages["invoice"] = True
                    break
                    
        if all(stages.values()):
            print("\n🎉 END-TO-END TEST PASSED!")
            return True
            
        time.sleep(10)
        print(f"   ... still waiting ({int(time.time() - start_time)}s)")

    print("\n❌ TEST TIMEOUT OR FAILED.")
    print(f"Final States: {stages}")
    return False

if __name__ == "__main__":
    run_test()
