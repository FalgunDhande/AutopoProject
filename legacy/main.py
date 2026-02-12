import json
from db import *
from utils import *
from llm import llama
from email_templates import email_full, email_partial, email_production, email_clarification
from invoice_gen4 import generate_invoice_from_agent

def process_po(po_id):
    print(f"\n🔥 Processing PO {po_id}")

    po = get_po_header(po_id)
    if not po:
        print("❌ PO not found")
        return

    items = get_po_items(po_id)
    decisions = []

    # Inventory check
    for item in items:
        available = get_inventory(item["product_id"])
        status = check_inventory(item["requested_qty"], available)
        
        decisions.append({
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "requested": item["requested_qty"],
            "available": available,
            "decision": status,
            "allocatable": min(item["requested_qty"], available)
        })

    print("\n📌 Inventory Decisions:")
    print(json.dumps(decisions, indent=2))


    # ---------------- FULL ----------------
    if all(d["decision"] == "full" for d in decisions):
        send_mock_email("Full Invoice Ready", email_full(po))

        for d in decisions:
            update_inventory(d["product_id"], -d["requested"])

        path = generate_invoice_from_agent(po, decisions)
        print("📄 FULL INVOICE:", path)

        update_po_status(po_id, "invoiced")
        log_event("full_invoice", f"PO {po_id} invoiced")
        return


    # ---------------- NONE ----------------
    if all(d["decision"] == "none" for d in decisions):
        send_mock_email("Production Needed", email_production(po))
        update_po_status(po_id, "production_needed")
        log_event("prod_full", f"PO {po_id} needs production")
        return


    # ---------------- PARTIAL ----------------
    send_mock_email("Approval Required", email_partial(po))

    reply = input("📨 Client reply: ").strip()
    classification = llama(
        f"Classify as approve/reject/clarify: '{reply}'"
    ).lower()

    # APPROVE
    if classification == "approve":
        alloc = []
        for d in decisions:
            if d["allocatable"] > 0:
                update_inventory(d["product_id"], -d["allocatable"])
                alloc.append(d)

        path = generate_invoice_from_agent(po, alloc)
        send_mock_email("Partial Invoice Ready", f"Invoice: {path}")

        update_po_status(po_id, "partial_approved")
        log_event("partial_yes", f"PO {po_id} partial approved")
        return

    # REJECT
    if classification == "reject":
        send_mock_email("Production Needed", email_production(po))
        update_po_status(po_id, "production_required")
        log_event("partial_no", f"PO {po_id} needs production")
        return

    # CLARIFY
    send_mock_email("Need Clarification", email_clarification(reply))
    update_po_status(po_id, "clarification_required")
    log_event("clarify", f"PO {po_id} clarification requested")
