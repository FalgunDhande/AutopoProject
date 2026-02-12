
import schedule
import time
import subprocess
import os
from datetime import datetime

PYTHON_EXEC = "/Users/garvitgupta/Desktop/po_pipeline/venv/bin/python"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_sales_history_update():
    print(f"\n[SCHEDULER] Running Sales History Update at {datetime.now()}")
    try:
        subprocess.run([PYTHON_EXEC, os.path.join(BASE_DIR, "..", "ml", "update_sales_history.py")], check=True)
        print("[SCHEDULER] ✅ Sales History Updated.")
    except Exception as e:
        print(f"[SCHEDULER] ❌ Failed to update sales history: {e}")

def run_demand_forecasting():
    print(f"\n[SCHEDULER] Running Demand Forecasting at {datetime.now()}")
    try:
        subprocess.run([PYTHON_EXEC, os.path.join(BASE_DIR, "..", "ml", "demand_season.py")], check=True)
        print("[SCHEDULER] ✅ Demand Forecast Updated.")
    except Exception as e:
        print(f"[SCHEDULER] ❌ Failed to update demand forecast: {e}")

def job():
    print("--- Starting Scheduled ML Maintenance ---")
    run_sales_history_update()
    run_demand_forecasting()
    print("--- Scheduled ML Maintenance Completed ---\n")

if __name__ == "__main__":
    # For demonstration, run immediately once
    job()
    
    # Schedule to run every day at midnight (example)
    # schedule.every().day.at("00:00").do(job)
    
    print("Scheduler is running. Press Ctrl+C to exit.")
    
    # Loop
    while True:
        schedule.run_pending()
        time.sleep(60)
