
import sys
import os
from unittest.mock import MagicMock, patch
from reportlab.pdfgen import canvas

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Create dummy PDF for attachment
DUMMY_PDF = "/app/test_invoice.pdf"
def create_dummy_pdf():
    try:
        c = canvas.Canvas(DUMMY_PDF)
        c.drawString(100, 750, "TEST INVOICE")
        c.save()
        print(f"Created dummy PDF at {DUMMY_PDF}")
    except Exception as e:
        print(f"Failed to create PDF: {e}")

# We only mock DB interactions and Invoice Generation path
# We DO NOT mock send_email so real emails are sent

with patch('psycopg2.connect'), \
     patch('core.optimized_agent.generate_invoice_for_po') as mock_gen_invoice, \
     patch('core.optimized_agent.update_inventory_stock'), \
     patch('core.optimized_agent.update_po_status'):

    from core.optimized_agent import process_po

    mock_gen_invoice.return_value = DUMMY_PDF

    # Re-patching helpers to inject scenarios
    with patch('core.optimized_agent.get_po_details') as mock_get_details, \
         patch('core.optimized_agent.get_inventory_batch') as mock_get_stock:
         
        # We REMOVED mock_llm_body so it uses REAL LLM (requests.post)
        # Assuming Docker container has network access to localhost:11434 (which is po_ollama in compose network)
        # But wait, optimized_agent.py uses OLLAMA_URL env var.
        # In docker-compose, po_flask has OLLAMA_URL=http://po_ollama:11434/api/generate
        # So it SHOULD work.
        pass
        
        # Setup Test Data
        TEST_EMAIL = "garvit.cdac.mum.aug25@gmail.com"
        
        mock_get_details.return_value = (
            {
                "po_number": "PO-TEST-001",
                "buyer": "Test Client",
                "supplier": "Involexis",
                "total": 1000.00,
                "buyer_email": TEST_EMAIL
            },
            [{"product_id": "P1", "product_name": "Item A", "requested": 10, "unit_price": 100}]
        )

        create_dummy_pdf()
        
        print("\n--- Sending FULL STOCK Email ---")
        mock_get_stock.return_value = {"P1": 20}
        process_po(1)

        print("\n--- Sending PARTIAL STOCK Email ---")
        mock_get_stock.return_value = {"P1": 5}
        process_po(2)

        print("\n--- Sending NO STOCK Email ---")
        mock_get_stock.return_value = {"P1": 0}
        process_po(3)
