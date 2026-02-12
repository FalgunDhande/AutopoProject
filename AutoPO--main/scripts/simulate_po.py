import os
import json
import sys
from datetime import datetime

# Add script dir to path to import generate_test_po
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generate_test_po import create_test_po

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCOMING_DIR = os.path.join(BASE_DIR, "incoming")
MANIFEST_FILE = os.path.join(BASE_DIR, "manifest.json")

def simulate():
    # Ensure directory exists
    os.makedirs(INCOMING_DIR, exist_ok=True)

    # 1. Generate PDF
    timestamp = int(datetime.now().timestamp())
    po_number = f"PO-SIM-{timestamp}"
    filename = f"{po_number}.pdf"
    file_path = os.path.join(INCOMING_DIR, filename)
    
    print(f"Generating test PO: {filename}")
    try:
        create_test_po(file_path, po_number=po_number)
    except Exception as e:
        print(f"❌ Failed to generate PDF: {e}")
        return
    
    # 2. Update Manifest
    if os.path.exists(MANIFEST_FILE):
        try:
            with open(MANIFEST_FILE, 'r') as f:
                manifest = json.load(f)
        except json.JSONDecodeError:
             manifest = {}
    else:
        manifest = {}
        
    manifest[filename] = {
        "status": "pending",
        "email_metadata": {
            "from_email": "simulation@test.com",
            # Format expected by pipeline might vary, but let's try standard format
            "received_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0000"),
            "subject": f"Purchase Order {po_number}"
        }
    }
    
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(manifest, f, indent=2)
        
    print(f"✅ Created {filename} in incoming/ and updated manifest.json")
    print("🚀 OCR Worker should pick this up immediately!")

if __name__ == "__main__":
    simulate()
