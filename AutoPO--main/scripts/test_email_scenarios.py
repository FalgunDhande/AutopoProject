
import sys
import os
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock dependencies BEFORE importing the module under test
# We need to mock database and email sending to avoid side effects

with patch('psycopg2.connect') as mock_connect, \
     patch('core.optimized_agent.send_email') as mock_send_email, \
     patch('core.optimized_agent.generate_invoice_for_po') as mock_gen_invoice, \
     patch('core.optimized_agent.update_inventory_stock') as mock_update_stock, \
     patch('core.optimized_agent.update_po_status') as mock_update_status:

    from core.optimized_agent import process_po

    # Setup Mocks
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_gen_invoice.return_value = "/tmp/dummy_invoice.pdf"

    def run_test(scenario_name, stock_qty, req_qty=10):
        print(f"\n--- Testing Scenario: {scenario_name} (Req: {req_qty}, Stock: {stock_qty}) ---")
        
        # Mock PO Data
        # Header: number, buyer, supplier, total, raw_json, sender_email
        mock_cursor.fetchone.return_value = (
            "PO-TEST-001", "Test Buyer Corp", "Supplier Inc", 1000.00, None, "buyer@test.com"
        )
        
        # Items: id, name, qty, price
        # Note: get_po_details calls fetchone (header) then fetchall (items)
        # We need to configure side_effect for execute? No, we mock the return values of fetch* calls.
        # process_po calls get_po_details -> fetchone (header), fetchall (items)
        # Then get_inventory_batch -> fetchall (stock)
        
        # We need to be careful with call order. 
        # Easier: Patch get_po_details and get_inventory_batch directly in optimized_agent
        pass

    # Re-patching internal helpers for easier control
    with patch('core.optimized_agent.get_po_details') as mock_get_details, \
         patch('core.optimized_agent.get_inventory_batch') as mock_get_stock:
        
        # Setup Common Data
        mock_get_details.return_value = (
            {
                "po_number": "PO-TEST-001",
                "buyer": "Test Buyer Corp",
                "supplier": "Supplier Inc",
                "total": 1000.00,
                "buyer_address": "123 Lane",
                "buyer_email": "buyer@test.com"
            },
            [
                {"product_id": "PROD-001", "product_name": "Widget", "requested": 10, "unit_price": 100.0}
            ]
        )

        # 1. Full Stock Case
        mock_get_stock.return_value = {"PROD-001": 20} # 20 > 10
        process_po(123)
        
        # detailed print
        if mock_send_email.called:
            args = mock_send_email.call_args[0]
            print(f"SUBJECT: {args[1]}")
            print(f"BODY:\n{args[2]}")
        else:
            print("No email sent!")
        mock_send_email.reset_mock()

        # 2. Partial Stock Case
        mock_get_stock.return_value = {"PROD-001": 5} # 5 < 10
        process_po(123)
        
        if mock_send_email.called:
            args = mock_send_email.call_args[0]
            print(f"SUBJECT: {args[1]}")
            print(f"BODY:\n{args[2]}")
        else:
            print("No email sent!")
        mock_send_email.reset_mock()

        # 3. No Stock Case
        mock_get_stock.return_value = {"PROD-001": 0} # 0
        process_po(123)
        
        if mock_send_email.called:
            args = mock_send_email.call_args[0]
            print(f"SUBJECT: {args[1]}")
            print(f"BODY:\n{args[2]}")
        else:
            print("No email sent!")
        mock_send_email.reset_mock()

