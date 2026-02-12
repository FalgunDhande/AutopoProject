import time
import json
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.po_ocr_worker import run_ocr

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INCOMING = os.path.join(BASE_DIR, "incoming")
MANIFEST = os.path.join(BASE_DIR, "manifest.json")


def load_manifest():
    try:
        with open(MANIFEST) as f:
            return json.load(f)
    except:
        return {}


def run():
    print(f"OCR service started. Watching: {INCOMING}")
    while True:
        manifest = load_manifest()
        for f, v in manifest.items():
            if v.get("status") == "pending":
                file_path = os.path.join(INCOMING, f)
                if os.path.exists(file_path):
                    run_ocr(f)
        time.sleep(2)


if __name__ == "__main__":
    run()