import psycopg2, select
from Invoice_Agent import process_po
from db import DB_CONFIG

def listen_for_po_events():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("LISTEN new_po_channel;")

    print("👂 Listening for pending POs...")

    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue

        conn.poll()

        while conn.notifies:
            n = conn.notifies.pop(0)
            po_id = int(n.payload)

            print(f"\n🔥 EVENT: New pending PO detected → {po_id}")
            process_po(po_id)

if __name__ == "__main__":
    listen_for_po_events()
