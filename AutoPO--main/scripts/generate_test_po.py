from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
import os

def create_test_po(filename, po_number="PO-VERIFY-999"):
    c = canvas.Canvas(filename, pagesize=A4)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, 800, "PURCHASE ORDER")
    
    c.setFont("Helvetica", 12)
    c.drawString(50, 770, f"PO Number: {po_number}")
    c.drawString(50, 755, "Date: 2026-01-30")
    
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, 720, "Buyer:")
    c.setFont("Helvetica", 12)
    c.drawString(50, 705, "Test Buyer Corp")
    c.drawString(50, 690, "123 Test Street, Mumbai")
    c.drawString(50, 675, "GST: 27AAAAA0000A1Z5")
    
    c.setFont("Helvetica-Bold", 12)
    c.drawString(300, 720, "Seller:")
    c.setFont("Helvetica", 12)
    c.drawString(300, 705, "Involexis AI Solutions")
    c.drawString(300, 690, "Tech Park, Bangalore")
    c.drawString(300, 675, "GST: 29BBBBB1111B2Z6")
    
    # Table Header
    c.line(50, 640, 550, 640)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(50, 625, "Product ID")
    c.drawString(150, 625, "Description")
    c.drawString(350, 625, "Qty")
    c.drawString(400, 625, "Price")
    c.drawString(480, 625, "Total")
    c.line(50, 620, 550, 620)
    
    # Item 1
    c.setFont("Helvetica", 10)
    c.drawString(50, 605, "FKP0000001")
    c.drawString(150, 605, "Industrial Bearing X1")
    c.drawString(350, 605, "5")
    c.drawString(400, 605, "1200.00")
    c.drawString(480, 605, "6000.00")
    
    # Item 2
    c.drawString(50, 590, "FKP0000002")
    c.drawString(150, 590, "Hydraulic Seal Kit")
    c.drawString(350, 590, "2")
    c.drawString(400, 590, "2500.00")
    c.drawString(480, 590, "5000.00")
    
    c.line(50, 570, 550, 570)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(400, 550, "Total Amount:")
    c.drawString(480, 550, "11000.00")
    
    c.save()
    print(f"✅ Test PO created: {filename}")

if __name__ == "__main__":
    os.makedirs("test_assets", exist_ok=True)
    create_test_po("test_assets/test_po_999.pdf")
