import imaplib
import email
import time
from dotenv import load_dotenv
import requests
from email.header import decode_header
import re
from datetime import datetime
import sys
import os

# Load local environment variables
load_dotenv()

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_CONFIG
import psycopg2

# Gmail Credentials from environment
IMAP_SERVER = "imap.gmail.com"
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

OLLAMA_URL = "http://localhost:11434/api/generate"

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def classify_intent(email_body):
    """
    Uses LLM to classify the customer's reply.
    """
    prompt = f"""
    You are an AI assistant classifying customer replies regarding a Partial Order Proposal.
    
    The customer was asked: "We have partial stock. Should we ship available items?"
    
    Classify the following reply into exactly one of these categories:
    - APPROVE (Customer wants us to ship available items, e.g., "Yes", "Go ahead", "Ship it", "Okay", "Proceed")
    - REJECT (Customer does NOT want partial shipment, e.g., "No", "Cancel", "Wait for full stock", "Don't ship")
    - OTHER (Unclear, asking for more info, or unrelated)
    
    REPLY: "{email_body[:500]}"
    
    OUTPUT ONLY THE CATEGORY NAME (APPROVE, REJECT, or OTHER). Do not add any explanation.
    """
    
    try:
        res = requests.post(OLLAMA_URL, json={
            "model": "qwen2.5:7b",
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0} # Deterministic
        }, timeout=30)
        
        intent = res.json().get("response", "").strip().upper()
        
        # Cleanup potential extra text
        if "APPROVE" in intent: return "APPROVE"
        if "REJECT" in intent: return "REJECT"
        return "OTHER"
        
    except Exception as e:
        print(f"LLM Classification Error: {e}")
        return "OTHER"

def find_po_in_subject(subject):
    """
    Extract PO number from subject (assuming format like "Re: Proposal for PO-1234")
    """
    # Simple regex for PO-XXXX or just finding numbers if strictly formatted
    # Adjust based on your PO Number format
    match = re.search(r"(PO-\w+|[0-9]+)", subject, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def process_replies():
    print("📧 Checking for replies...")
    
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("INBOX")
    
    # Search for UNSEEN emails
    status, messages = mail.search(None, 'UNSEEN')
    
    if not messages[0]:
        print("No new emails.")
        return

    for mail_id in messages[0].split():
        _, msg_data = mail.fetch(mail_id, '(RFC822)')
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding or "utf-8")
                
                print(f"Processing: {subject}")
                
                # extracting plain text body
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode()
                            break
                else:
                    body = msg.get_payload(decode=True).decode()

                # 1. Identify PO
                po_number = find_po_in_subject(subject)
                if not po_number:
                    # Fallback: check if the PO number is in the body
                    match = re.search(r"(PO-\w+|[0-9]{4,})", body, re.IGNORECASE)
                    if match:
                        po_number = match.group(1)

                if not po_number:
                    print(f"Skipping: No PO number found in subject or body for '{subject}'")
                    continue

                # 2. Classify Intent
                intent = classify_intent(body)
                print(f"Detected Intent: {intent} for PO {po_number}")
                
                # 3. Trigger Agent Action
                if intent in ["APPROVE", "REJECT"]:
                    from core.optimized_agent import handle_partial_response
                    handle_partial_response(po_number, intent)
                    
                    # Mark email as seen only if processed
                    mail.store(mail_id, '+FLAGS', '\\Seen')
                else:
                    print("Intent unclear. Manual review needed.")

    mail.close()
    mail.logout()

if __name__ == "__main__":
    while True:
        try:
            process_replies()
        except Exception as e:
            print(f"Error in reply listener loop: {e}")
        time.sleep(30)
