
import os
import json
import psycopg2
from datetime import datetime
from config.db_config import DB_CONFIG

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle,
    Paragraph, Spacer, Image
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

# ============================================================
# 1. DATABASE CONFIG
# ============================================================

def db_connect():
    return psycopg2.connect(**DB_CONFIG)

def get_product_data(product_id):
    """Fetch unit price and name from inventory table."""
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT price, product_name FROM inventory WHERE product_id = %s", (product_id,))
    row = cur.fetchone()

    conn.close()
    if row:
        return {"price": float(row[0]), "name": row[1]}
    return {"price": 0.0, "name": "Unknown Product"}

# ============================================================
# 2. COMPANY DETAILS LOADER
# ============================================================

def load_company():
    comp_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "company_info.json")
    try:
        with open(comp_file, "r") as f:
            return json.load(f)
    except:
        return {
            "name": "Default Company",
            "address": "Address not found",
            "phone": "",
            "email": ""
        }

# ============================================================
# 3. INVOICE GENERATOR CLASS
# ============================================================

class InvoiceGenerator:

    def generate(self, company, data, po_number, is_partial=False, output_dir="invoices"):
        os.makedirs(output_dir, exist_ok=True)

        suffix = "PARTIAL" if is_partial else "FULL"
        filename = f"{po_number}_{suffix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
        pdf_path = os.path.join(output_dir, filename)

        self.build_pdf(company, data, pdf_path)
        return pdf_path

    def build_pdf(self, company, data, pdf_path):

        buyer = data["buyer"]
        po = data["po"]
        items = data["items"]

        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=letter,
            rightMargin=72, leftMargin=72,
            topMargin=72, bottomMargin=36
        )

        styles = getSampleStyleSheet()
        
        # Using standard fonts to avoid path issues
        header_style = styles["Heading1"]
        normal_style = styles["Normal"]

        story = []

        # LOGO (Optional)
        logo_path = os.path.join(os.path.dirname(__file__), "logo.jpg")
        if os.path.exists(logo_path):
            story.append(Image(logo_path, width=120, height=50))
        story.append(Spacer(1, 10))

        # COMPANY HEADER
        story.append(Paragraph(f"<b>{company.get('name', '')}</b>", header_style))
        story.append(Paragraph(company.get("address", ""), normal_style))
        story.append(Paragraph(f"Phone: {company.get('phone', '')}", normal_style))
        story.append(Paragraph(f"Email: {company.get('email', '')}", normal_style))
        story.append(Spacer(1, 20))

        # TITLE
        title_style = ParagraphStyle(
            "TitleCentered", parent=styles["Title"], alignment=TA_CENTER
        )
        title_text = "PARTIAL INVOICE" if "PARTIAL" in pdf_path else "TAX INVOICE"
        story.append(Paragraph(title_text, title_style))
        story.append(Spacer(1, 20))

        # BILL TO & PO INFO
        story.append(Paragraph(f"<b>Bill To:</b> {buyer.get('name', 'N/A')}", normal_style))
        if buyer.get("address"):
             story.append(Paragraph(buyer.get("address", ""), normal_style))
        if buyer.get("gst"):
            story.append(Paragraph(f"GSTIN: {buyer.get('gst')}", normal_style))
        
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"PO Number: {po.get('number', 'N/A')}", normal_style))
        story.append(Paragraph(f"Date: {datetime.now().strftime('%Y-%m-%d')}", normal_style))
        story.append(Spacer(1, 20))

        # TABLE
        table_data = [["Description", "Qty", "Unit Price", "Amount"]]
        subtotal = 0

        for it in items:
            product_id = it.get("product_id")
            # If price/name missing in PO, fetch from DB
            db_data = get_product_data(product_id)
            
            desc = it.get("description") or db_data["name"]
            price = float(it.get("unit_price") or db_data["price"])
            qty = float(it.get("quantity", 0))
            
            amount = qty * price
            subtotal += amount

            table_data.append([
                desc[:40], # Truncate long names
                str(int(qty)),
                f"{price:.2f}",
                f"{amount:.2f}"
            ])

        table = Table(table_data, colWidths=[3*inch, 1*inch, 1.2*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.grey),
            ("TEXTCOLOR", (0,0), (-1,0), colors.whitesmoke),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("BOTTOMPADDING", (0,0), (-1,0), 12),
            ("GRID", (0,0), (-1,-1), 1, colors.black),
        ]))

        story.append(table)
        story.append(Spacer(1, 25))

        # TOTALS
        cgst = subtotal * 0.09
        sgst = subtotal * 0.09
        total = subtotal + cgst + sgst

        totals = [
            ["Subtotal:", f"{subtotal:.2f}"],
            ["CGST (9%):", f"{cgst:.2f}"],
            ["SGST (9%):", f"{sgst:.2f}"],
            ["TOTAL:", f"{total:.2f}"],
        ]

        t_table = Table(totals, colWidths=[3*inch, 2.5*inch])
        t_table.setStyle(TableStyle([
            ("ALIGN", (1,0), (-1,-1), "RIGHT"),
            ("FONTNAME", (-1,-1), (-1,-1), "Helvetica-Bold"),
        ]))

        story.append(t_table)
        story.append(Spacer(1, 20))
        story.append(Paragraph("Thank you for your business!", normal_style))

        doc.build(story)

# ============================================================
# 4. AGENT WRAPPER
# ============================================================

def generate_invoice_for_po(po_id, po_header, alloc_items):
    """
    Called by Agent. Generates PDF.
    """
    company = load_company()

    buyer = {
        "name": po_header.get("buyer", "Unknown Buyer"),
        "gst": po_header.get("buyer_gst", ""),
        "address": po_header.get("buyer_address", "")
    }

    po_block = {
        "number": po_header.get("po_number", str(po_id)),
    }

    # Clean items for generator
    clean_items = []
    for item in alloc_items:
        clean_items.append({
            "product_id": item["product_id"],
            "description": item["product_name"],
            "quantity": item["allocatable"],
            "unit_price": item.get("unit_price", 0) # Passed optionally
        })

    is_partial = any(item["allocatable"] != item["requested"] for item in alloc_items)

    gen = InvoiceGenerator()
    path = gen.generate(company, {"buyer": buyer, "po": po_block, "items": clean_items}, po_block["number"], is_partial)
    
    return path
