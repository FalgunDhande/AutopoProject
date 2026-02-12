#!/bin/bash

echo "🛑 Stopping PO Pipeline Project..."

# Kill processes by matching the command line
pkill -f "flask_app/app.py"
pkill -f "services/email_ingestion_imap.py"
pkill -f "services/po_ocr_worker_service.py"
pkill -f "services/reply_listener.py"
pkill -f "services/scheduler.py"

echo "✅ All services stopped."
