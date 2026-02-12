
import sys
import os
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock dependencies
# We want to use REAL LLM generation logic in optimized_agent.py
# So we only mock DB and Email

def print_email(to, subject, body, pdf=None):
    print(f"\n📨 [MOCK EMAIL] To: {to}")
    print(f"Subject: {subject}")
    print(f"Body:\n{'-'*20}\n{body}\n{'-'*20}\n")

with patch('psycopg2.connect'), \
     patch('core.optimized_agent.generate_invoice_for_po', return_value="/app/test.pdf"), \
     patch('core.optimized_agent.update_inventory_stock'), \
     patch('core.optimized_agent.update_po_status'), \
     patch('core.optimized_agent.send_email', side_effect=print_email):

    from core.optimized_agent import process_po

    # Patch data helpers
    with patch('core.optimized_agent.get_po_details') as mock_get_details, \
         patch('core.optimized_agent.get_inventory_batch') as mock_get_stock:
         
        # Setup Test Data
        mock_get_details.return_value = (
            {
                "po_number": "PO-DEBUG-001",
                "buyer": "Debug Client Corp",
                "supplier": "Involexis",
                "total": 5000.00,
                "buyer_email": "debug@test.com"
            },
            [{"product_id": "P1", "product_name": "Mega Widget", "requested": 100, "unit_price": 50}]
        )

        # Partial Stock Scenario
        # Request 100, Available 40
        mock_get_stock.return_value = {"P1": 40}
        
        print("\n--- TRIGGERING PARTIAL STOCK (LLM GENERATION) ---")
        process_po(999)

        # No Stock Scenario
        mock_get_stock.return_value = {"P1": 0}
        
        print("\n--- TRIGGERING NO STOCK (LLM GENERATION) ---")
        process_po(998)
