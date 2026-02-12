
import os
import json
import sys
import psycopg2

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_CONFIG
from core.db_insert import insert_po

PROCESSED_DIR = "processed_json"

def sync():
    print(f"🔄 Starting Sync: {PROCESSED_DIR} -> Database")
    
    # Connect to check existing
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get all JSON files
    files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".json")]
    print(f"📂 Found {len(files)} JSON files.")
    
    # Clear existing POs to avoid duplicates for this clean sync
    # WARNING: This is a deep reset for visibility
    print("🗑️ Cleaning existing PO data for fresh sync...")
    cur.execute("TRUNCATE purchase_orders CASCADE;")
    conn.commit()
    
    synced_count = 0
    for file_name in files:
        file_path = os.path.join(PROCESSED_DIR, file_name)
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Use the existing insertion logic
            insert_po(data)
            synced_count += 1
            print(f"✅ Synced: {file_name}")
            
        except Exception as e:
            print(f"❌ Failed to sync {file_name}: {e}")
            
    conn.close()
    print(f"\n✨ Sync completed! Total records added: {synced_count}")

if __name__ == "__main__":
    sync()
