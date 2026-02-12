import os
import json
import shutil
import requests
import pdfplumber
import numpy as np
import cv2

from pdf2image import convert_from_path
from paddleocr import PaddleOCR

from core.db_insert import insert_po


# ================= CONFIG ================= #

# Point to project root, not core/ directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INCOMING = os.path.join(BASE_DIR, "incoming")
PROCESSING = os.path.join(BASE_DIR, "processing")
OUTPUT = os.path.join(BASE_DIR, "processed_json")
FAILED = os.path.join(BASE_DIR, "failed")
MANIFEST = os.path.join(BASE_DIR, "manifest.json")

OLLAMA_URL = "http://localhost:11434/api/generate"
LLM_MODEL = "qwen2.5:7b"

# ========================================= #

os.makedirs(PROCESSING, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)
os.makedirs(FAILED, exist_ok=True)

# Optimized PaddleOCR Settings
# use_angle_cls=False for speed if orientation is fixed
# limit_side_len=1280 for faster processing of large images
ocr = PaddleOCR(lang="en", use_angle_cls=False)



# ================= MANIFEST ================= #

def load_manifest():
    try:
        with open(MANIFEST) as f:
            return json.load(f)
    except:
        return {}


def save_manifest(m):
    with open(MANIFEST, "w") as f:
        json.dump(m, f, indent=2)


# ================= OCR ================= #

def extract_text_from_pdf(pdf_path):
    """
    1. Try digital PDF text extraction
    2. Fallback to PaddleOCR for scanned PDFs
    """
    print(f"📄 Extracting text from: {os.path.basename(pdf_path)}")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text() or ""
            # If we get enough text digitally, skip heavy OCR
            if len(first_page_text.strip()) > 100:
                print("⚡ Using digital text extraction (Fast)")
                return "\n".join(p.extract_text() or "" for p in pdf.pages[:3])
    except Exception as e:
        print(f"⚠️ Digital extraction failed: {e}")

    print("🖼️ Digital extraction yielded little text. Falling back to Scanned OCR (Slower)...")
    text = ""
    # Use multiple threads for PDF conversion and lower DPI for speed
    images = convert_from_path(pdf_path, dpi=150, first_page=1, last_page=3, thread_count=4)

    for i, img in enumerate(images):
        print(f"📸 Processing page {i+1}...")
        gray = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2GRAY)
        # Use simple OCR call for speed
        result = ocr.ocr(gray, cls=False)
        if result:
            for line in result:
                if line:
                    for res in line:
                        text += res[1][0] + " "
                    text += "\n"

    return text




# ================= LLM ================= #

def extract_po_with_llm(text):
    prompt = f"""
You are extracting structured Purchase Order data from noisy OCR text.

CRITICAL RULES:
- Return ONLY valid JSON
- Do NOT hallucinate values
- If a field is missing, return empty string ""
- Do NOT mix fields (dates must not appear as quantity)
- Monetary fields must contain ONLY numbers (no commas, no currency symbols)
- Preserve vendor text in raw fields

RETURN JSON IN THIS EXACT STRUCTURE:

{{
  "po_number": "",
  "po_date": "",
  "buyer": {{
    "company_name": "",
    "gst_number": "",
    "address": "",
    "email": ""
  }},
  "seller": {{
    "company_name": "",
    "gst_number": "",
    "address": ""
  }},
  "currency": "",
  "total_amount": "",
  "line_items": [
    {{
      "product_id": "",
      "product_identifier_raw": "",
      "product_identifier_type": "",
      "description": "",
      "unit": "",
      "quantity": "",
      "unit_price": "",
      "line_total": ""
    }}
  ]
}}

FIELD GUIDELINES:
- product_id: internal identifier if present, else empty
- product_identifier_raw: vendor-provided code, SAC/HSN, or combined text
- product_identifier_type: classify raw identifier (e.g. HSN/SAC, SKU, Service Description)
- quantity: numeric quantity only
- unit_price, line_total, total_amount: numeric only
- currency: ISO code like INR, USD

OCR TEXT:
{text[:4500]}
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": LLM_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0}
        },
        timeout=120
    )

    raw = response.json()["response"]
    return json.loads(raw[raw.find("{"): raw.rfind("}") + 1])


# ================= MAIN WORKER ================= #

def run_ocr(file_name):
    manifest = load_manifest()

    if file_name not in manifest:
        return

    if manifest[file_name]["status"] != "pending":
        return

    src = os.path.join(INCOMING, file_name)
    if not os.path.exists(src):
        return

    print(f"OCR started for: {file_name}")

    proc = os.path.join(PROCESSING, file_name)
    shutil.move(src, proc)

    try:
        text = extract_text_from_pdf(proc)
        print(f"OCR text length: {len(text)}")

        extracted_data = extract_po_with_llm(text)

        final_json = {
            "file_name": file_name,
            "email_metadata": manifest[file_name].get("email_metadata", {}),
            "llm_model": LLM_MODEL,
            "extracted_data": extracted_data
        }

        json_name = file_name.rsplit(".", 1)[0] + ".json"
        with open(os.path.join(OUTPUT, json_name), "w") as f:
            json.dump(final_json, f, indent=2)

        insert_po(final_json)

        manifest[file_name]["status"] = "processed"
        manifest[file_name]["json"] = json_name
        save_manifest(manifest)

        print(f"OCR completed successfully: {file_name}")

    except Exception as e:
        print(f"OCR failed for {file_name}: {e}")

        manifest[file_name]["status"] = "failed"
        manifest[file_name]["error"] = str(e)
        save_manifest(manifest)

        shutil.move(proc, os.path.join(FAILED, file_name))