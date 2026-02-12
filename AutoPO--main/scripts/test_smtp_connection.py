import smtplib
import os
from email.mime.text import MIMEText

# Load env from .env file manually since we are running as script
def load_env():
    try:
        with open('.env') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, val = line.strip().split('=', 1)
                    os.environ[key] = val
    except:
        pass

load_env()

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
TEST_RECIPIENT = "involexis.team@gmail.com"  # Using the same email to test loopback

print(f"--- SMTP DIAGNOSTIC ---")
print(f"User: {EMAIL_USER}")
print(f"Pass: {'*' * len(EMAIL_PASS) if EMAIL_PASS else 'NONE'}")

try:
    print("1. Connecting to SMTP Server...")
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.set_debuglevel(1)  # Enable debug output
    
    print("2. Starting TLS...")
    server.starttls()
    
    print("3. Logging in...")
    server.login(EMAIL_USER, EMAIL_PASS)
    print("✅ Login Successful!")
    
    msg = MIMEText("This is a direct SMTP test from the Docker container.")
    msg['Subject'] = "Docker SMTP Test"
    msg['From'] = EMAIL_USER
    msg['To'] = TEST_RECIPIENT
    
    print(f"4. Sending email to {TEST_RECIPIENT}...")
    server.sendmail(EMAIL_USER, TEST_RECIPIENT, msg.as_string())
    print("✅ Email Sent!")
    
    server.quit()
    
except Exception as e:
    print(f"\n❌ SMTP FAILED: {e}")
