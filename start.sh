#!/bin/bash

# Configuration
VENV_PYTHON="./venv/bin/python"
LOG_DIR="./logs"
mkdir -p $LOG_DIR

echo "🚀 Starting PO Pipeline Project..."

# 1. Start Flask App
nohup $VENV_PYTHON flask_app/app.py > $LOG_DIR/flask.log 2>&1 &
echo "✅ Flask App started (PID $!)"

# 2. Start Email Ingestion
nohup $VENV_PYTHON services/email_ingestion_imap.py > $LOG_DIR/ingestion.log 2>&1 &
echo "✅ Email Ingestion started (PID $!)"

# 3. Start OCR Worker
nohup $VENV_PYTHON services/po_ocr_worker_service.py > $LOG_DIR/ocr.log 2>&1 &
echo "✅ OCR Worker started (PID $!)"

# 4. Start Reply Listener
nohup $VENV_PYTHON services/reply_listener.py > $LOG_DIR/reply_listener.log 2>&1 &
echo "✅ Reply Listener started (PID $!)"

# 5. Start Scheduler
nohup $VENV_PYTHON services/scheduler.py > $LOG_DIR/scheduler.log 2>&1 &
echo "✅ Scheduler started (PID $!)"

echo "------------------------------------------------"
echo "Project is running in the background."
echo "Dashboard: http://localhost:5001"
echo "Logs: $LOG_DIR/"
echo "Use ./stop.sh to stop all services."
