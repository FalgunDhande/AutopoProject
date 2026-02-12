import imaplib
import email
from email.header import decode_header
import os
import json
import uuid
import time
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from dotenv import load_dotenv

# Load local environment variables
load_dotenv()

# ================== CONFIG ================== #

IMAP_SERVER = "imap.gmail.com"
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCOMING = os.path.join(BASE_DIR, "incoming")
LOGS = os.path.join(BASE_DIR, "logs")
MANIFEST = os.path.join(BASE_DIR, "manifest.json")

ALLOWED_EXT = (".pdf", ".docx", ".doc", ".jpg", ".jpeg", ".png")

INITIAL_POLL = 5          # seconds
MAX_POLL = 1800           # 30 minutes
MIN_PO_SCORE = 3          # semantic threshold

# ============================================ #

os.makedirs(INCOMING, exist_ok=True)
os.makedirs(LOGS, exist_ok=True)


# ================== LOGGING ================== #

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(os.path.join(LOGS, "email.log"), "a") as f:
        f.write(line + "\n")


# ================== MANIFEST ================== #

def load_manifest():
    try:
        with open(MANIFEST) as f:
            return json.load(f)
    except:
        return {}

def save_manifest(m):
    with open(MANIFEST, "w") as f:
        json.dump(m, f, indent=2)


# ================== PO DETECTION ================== #

def looks_like_po(text):
    """
    Semantic, wording-independent PO detection
    """
    signals = [
        "bill to", "supplier", "vendor", "total",
        "amount", "price", "quantity", "gst",
        "item", "service"
    ]
    text = text.lower()
    score = sum(1 for s in signals if s in text)
    return score >= MIN_PO_SCORE


# ================== EMAIL BODY → PDF ================== #

def email_body_to_pdf(text, output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    y = 800
    for line in text.splitlines():
        if line.strip():
            c.drawString(40, y, line)
            y -= 14
            if y < 50:
                c.showPage()
                y = 800
    c.save()


# ================== EMAIL POLLING ================== #

def poll_emails(poll_interval):
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("INBOX")

    status, msgs = mail.search(None, "UNSEEN")
    ids = msgs[0].split()

    if not ids:
        mail.logout()
        # Return False but keep the same polling speed (User Requirement)
        return False, INITIAL_POLL

    mail_id = ids[-1]
    _, data = mail.fetch(mail_id, "(RFC822)")

    for resp in data:
        if not isinstance(resp, tuple):
            continue

        msg = email.message_from_bytes(resp[1])
        from_email = msg.get("From", "")
        received_at = msg.get("Date", "")

        body_text = ""
        attachment_saved = False

        for part in msg.walk():
            # -------- EMAIL BODY -------- #
            if part.get_content_type() == "text/plain":
                body_text += part.get_payload(decode=True).decode(errors="ignore")

            # -------- ATTACHMENTS -------- #
            if part.get_content_disposition() == "attachment":
                name = part.get_filename()
                if not name:
                    continue

                name = decode_header(name)[0][0]
                if isinstance(name, bytes):
                    name = name.decode()

                if name.lower().endswith(ALLOWED_EXT):
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    uid = uuid.uuid4().hex[:8]
                    ext = os.path.splitext(name)[1]
                    fname = f"{ts}_{uid}{ext}"

                    with open(os.path.join(INCOMING, fname), "wb") as f:
                        f.write(part.get_payload(decode=True))

                    manifest = load_manifest()
                    manifest[fname] = {
                        "status": "pending",
                        "email_metadata": {
                            "from_email": from_email,
                            "received_at": received_at
                        }
                    }
                    save_manifest(manifest)

                    log(f"New PO attachment saved: {fname}")
                    attachment_saved = True

        # -------- EMAIL BODY AS PO -------- #
        if not attachment_saved and looks_like_po(body_text):
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            uid = uuid.uuid4().hex[:8]
            fname = f"{ts}_{uid}_email_body.pdf"
            pdf_path = os.path.join(INCOMING, fname)

            email_body_to_pdf(body_text, pdf_path)

            manifest = load_manifest()
            manifest[fname] = {
                "status": "pending",
                "email_metadata": {
                    "from_email": from_email,
                    "received_at": received_at
                }
            }
            save_manifest(manifest)

            log(f"PO detected in email body, saved as: {fname}")

        mail.store(mail_id, "+FLAGS", "\\Seen")
        mail.logout()
        return True, INITIAL_POLL

    mail.logout()
    return False, INITIAL_POLL


# ================== MAIN LOOP ================== #

def run():
    poll_interval = INITIAL_POLL
    idle_logged = False

    log("Email ingestion service started")

    while True:
        try:
            found, poll_interval = poll_emails(poll_interval)

            if found:
                idle_logged = False
            else:
                if not idle_logged:
                    log("Waiting for new mail...")
                    idle_logged = True

            time.sleep(poll_interval)

        except KeyboardInterrupt:
            log("Email ingestion stopped")
            break

        except Exception as e:
            log(f"Error: {e}")
            time.sleep(poll_interval)


if __name__ == "__main__":
    run()