import os
import json
import psycopg2
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle,
    Paragraph, Spacer, Image
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# ============================================================
# 1. FONT REGISTRATION
# ============================================================

FONT_PATH = r"C:\Users\vijay\Downloads\dejavu-fonts-ttf-2.37\dejavu-fonts-ttf-2.37\ttf\DejaVuSans.ttf"
pdfmetrics.registerFont(TTFont("DejaVu", FONT_PATH))


# ============================================================
# 2. LOGO PATH
# ============================================================

LOGO_PATH = "logo.jpg"


# ============================================================
# 3. DATABASE CONFIG (for fetching price)
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": "Isha@2609"
}

def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def get_price(product_id):
    """Fetch unit price from inventory table."""
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT price FROM inventory WHERE product_id = %s", (product_id,))
    row = cur.fetchone()

    conn.close()
    return float(row[0]) if row else 0.0


# ============================================================
# 4. COMPANY DETAILS LOADER
# ============================================================

def load_company():
    with open("company_info.json", "r") as f:
        return json.load(f)


# ============================================================
# 5. INVOICE GENERATOR CLASS
# ============================================================

class InvoiceGenerator:

    def generate(self, company, data, po_number, is_partial=False, output_dir="invoices"):
        os.makedirs(output_dir, exist_ok=True)

        # File naming
        if is_partial:
            filename = f"{po_number}_PARTIAL_{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf"
        else:
            filename = f"{po_number}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf"

        pdf_path = os.path.join(output_dir, filename)

        self.build_pdf(company, data, pdf_path)
        return pdf_path


    # --------------------------------------------------------
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
        styles["Normal"].fontName = "DejaVu"
        styles["Title"].fontName = "DejaVu"

        story = []


        # ======================================================
        # LOGO + COMPANY DETAILS
        # ======================================================
        if os.path.exists(LOGO_PATH):
            story.append(Image(LOGO_PATH, width=120, height=50))

        story.append(Spacer(1, 10))

        company_style = ParagraphStyle(
            "CompanyTitle",
            parent=styles["Heading2"],
            fontName="DejaVu",
            fontSize=18,
            textColor=colors.HexColor("#1F6E8C"),
        )

        story.append(Paragraph(f"<b>{company['name']}</b>", company_style))
        story.append(Paragraph(company.get("tagline", ""), styles["Normal"]))
        story.append(Paragraph(company["address"], styles["Normal"]))
        story.append(Paragraph(f"Phone: {company['phone']}", styles["Normal"]))
        story.append(Paragraph(f"Email: {company['email']}", styles["Normal"]))
        if company.get("website"):
            story.append(Paragraph(company["website"], styles["Normal"]))

        story.append(Spacer(1, 20))


        # ======================================================
        # INVOICE TITLE
        # ======================================================
        title_style = ParagraphStyle(
            "TitleCentered",
            parent=styles["Title"],
            fontName="DejaVu",
            fontSize=26,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1F6E8C")
        )

        story.append(Paragraph("INVOICE", title_style))
        story.append(Spacer(1, 20))


        # ======================================================
        # BUYER INFO
        # ======================================================
        story.append(Paragraph(f"<b>Bill To:</b> {buyer.get('name', '')}", styles["Normal"]))
        story.append(Paragraph(f"GSTIN: {buyer.get('gst', '')}", styles["Normal"]))

        story.append(Spacer(1, 15))


        # ======================================================
        # PO INFO
        # ======================================================
        story.append(Paragraph(f"Invoice Date: {datetime.now().strftime('%Y-%m-%d')}", styles["Normal"]))
        story.append(Paragraph(f"PO Number: {po.get('number','')}", styles["Normal"]))
        story.append(Paragraph(f"PO Date: {po.get('date','')}", styles["Normal"]))

        story.append(Spacer(1, 20))


        # ======================================================
        # ITEMS TABLE
        # ======================================================
        table_data = [["Description", "Qty", "Unit Price", "Amount"]]

        subtotal = 0

        for it in items:
            qty = float(it.get("quantity", 0))
            price = float(it.get("unit_price", 0))
            amount = qty * price
            subtotal += amount

            table_data.append([
                it.get("description", ""),
                str(qty),
                f"₹{price:.2f}",
                f"₹{amount:.2f}"
            ])

        table = Table(table_data, colWidths=[3*inch, 1*inch, 1.2*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), "DejaVu"),
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#1F6E8C")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ]))

        story.append(table)
        story.append(Spacer(1, 25))


        # ======================================================
        # TAX & TOTALS
        # ======================================================
        cgst = subtotal * 0.09
        sgst = subtotal * 0.09
        total = subtotal + cgst + sgst

        totals = [
            ["Subtotal:", f"₹{subtotal:.2f}"],
            ["CGST (9%):", f"₹{cgst:.2f}"],
            ["SGST (9%):", f"₹{sgst:.2f}"],
            ["TOTAL:", f"₹{total:.2f}"],
        ]

        total_table = Table(totals, colWidths=[3*inch, 2.5*inch])
        total_table.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), "DejaVu"),
            ("ALIGN", (1,0), (-1,-1), "RIGHT"),
        ]))

        story.append(total_table)
        story.append(Spacer(1, 20))


        # ======================================================
        # FOOTER
        # ======================================================
        story.append(Paragraph("<b>Thank you for your business!</b>", styles["Normal"]))

        doc.build(story)


# ============================================================
# 6. WRAPPER for AGENT
# ============================================================

def generate_invoice_from_agent(po_header, decision_items):
    """
    Convert Agent output → Invoice data → PDF
    """

    company = load_company()

    # Buyer block
    buyer = {
        "name": po_header.get("buyer", ""),
        "gst": "",
        "address": "",
        "phone": "",
        "email": "",
    }

    # PO block
    po_block = {
        "number": po_header.get("po_number", ""),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "due_date": "",
        "currency": "INR"
    }

    # Items block (with REAL PRICE)
    invoice_items = []
    for item in decision_items:
        qty = item["allocatable"]
        price = get_price(item["product_id"])

        invoice_items.append({
            "description": item["product_name"],
            "quantity": qty,
            "unit_price": price
        })

    unified = {
        "buyer": buyer,
        "po": po_block,
        "items": invoice_items
    }

    is_partial = any(d["allocatable"] != d["requested"] for d in decision_items)

    generator = InvoiceGenerator()
    return generator.generate(
        company=company,
        data=unified,
        po_number=po_header.get("po_number"),
        is_partial=is_partial
    )
