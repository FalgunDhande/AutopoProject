import imaplib
import email
from email.header import decode_header
from pathlib import Path

# ================= CONFIG =================
IMAP_SERVER = "imap.gmail.com"
EMAIL_ID = "involexis.team@gmail.com"
EMAIL_PASSWORD = "jspk yczo ykes ctji"

SAVE_DIR = Path("AutoPO/data/download po")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ================= CONNECT =================
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ID, EMAIL_PASSWORD)
mail.select("inbox")

# ================= SEARCH =================
status, messages = mail.search(None, '(UNSEEN)')
mail_ids = messages[0].split()

print(f"📧 Unread mails found: {len(mail_ids)}")

for num in mail_ids:
    _, msg_data = mail.fetch(num, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])

    subject, enc = decode_header(msg["Subject"])[0]
    subject = subject.decode(enc or "utf-8") if isinstance(subject, bytes) else subject

    print(f"📩 Subject: {subject}")

    if "po" not in subject.lower() and "purchase" not in subject.lower():
        continue

    for part in msg.walk():
        if part.get_content_disposition() == "attachment":
            filename = part.get_filename()
            if filename and filename.lower().endswith(".pdf"):
                path = SAVE_DIR / filename
                with open(path, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"📄 Saved PO: {filename}")

mail.logout()
