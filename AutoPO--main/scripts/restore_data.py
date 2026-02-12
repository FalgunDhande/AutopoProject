import os
import json
import sys
from datetime import datetime

# Add project root to path so we can import core modules
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from core.db_insert import insert_po
from config.db_config import DB_CONFIG
import psycopg2

PROCESSED_JSON_DIR = os.path.join(BASE_DIR, "processed_json")

def restore_data():
    if not os.path.exists(PROCESSED_JSON_DIR):
        print(f"❌ Directory not found: {PROCESSED_JSON_DIR}")
        return

    files = [f for f in os.listdir(PROCESSED_JSON_DIR) if f.endswith(".json")]
    
    if not files:
        print("⚠️ No JSON files found to restore.")
        return

    print(f"🔄 Found {len(files)} files to restore...")
    
    success_count = 0
    
    for filename in files:
        filepath = os.path.join(PROCESSED_JSON_DIR, filename)
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Check if PO already exists to avoid duplicates
            # The insert_po function might handle this or fail with unique constraint
            # Let's trust insert_po or wrap in try-except
            
            print(f"Processing {filename}...")
            insert_po(data)
            success_count += 1
            
        except Exception as e:
            print(f"⚠️ Failed to restore {filename}: {e}")
            # If duplication error, that's fine, skip
            if "unique constraint" in str(e).lower():
                print("   (Already exists)")

    print(f"✅ Successfully restored {success_count}/{len(files)} records.")

if __name__ == "__main__":
    try:
        # Test DB connection first
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        restore_data()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
