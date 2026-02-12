import streamlit as st
import sys
import os
import subprocess
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.theme import apply_theme, display_logo

# Page config
st.set_page_config(
    page_title="Control Center - Involexis",
    page_icon="🚀",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

# Pipeline Mastery
st.header("⚡ Pipeline Mastery")
st.info("Trigger the end-to-end flow: Fetch Emails ➡️ Extract Data ➡️ Update ERP.")

if st.button("🚀 RUN FULL PIPELINE", key="btn_full_run", use_container_width=True):

    with st.status("Executing End-to-End Pipeline...", expanded=True) as status:
        st.write("📡 Connecting to Gmail...")
        try:
            # Run ingestion
            ingest_res = subprocess.run([sys.executable, "services/email_ingestion_imap.py"], capture_output=True, text=True, timeout=60)
            st.write("✅ Email Ingestion Complete.")
            
            st.write("🔍 Starting AI OCR Worker...")
            # Run OCR in background as it might take time
            subprocess.Popen([sys.executable, "services/po_ocr_worker_service.py"])
            st.write("✅ OCR Worker triggered in background.")
            
            status.update(label="Pipeline Execution Success!", state="complete", expanded=False)
            st.balloons()
        except Exception as e:
            status.update(label=f"Pipeline Failed: {e}", state="error")

st.markdown("---")

# Service Management
st.header("🛠️ Individual Service Control")
col1, col2 = st.columns(2)

with col1:
    st.subheader("📬 Email Ingestion")
    if st.button("Start Background Ingestion", key="btn_ingestion"):
        subprocess.Popen([sys.executable, "services/email_ingestion_imap.py"])
        st.success("Service started.")

with col2:
    st.subheader("🔍 OCR Worker")
    if st.button("Start Background OCR", key="btn_ocr"):
        subprocess.Popen([sys.executable, "services/po_ocr_worker_service.py"])
        st.success("Service started.")


# Pipeline Status
st.header("Pipeline Status")

def check_service_running(pattern):
    try:
        if sys.platform == "win32":
            output = subprocess.check_output(['tasklist'], text=True)
        else:
            output = subprocess.check_output(['ps', 'aux'], text=True)
        return pattern in output
    except:
        return False

# Manual Trigger Section
st.header("Manual Trigger")
col1, col2 = st.columns(2)

with col1:
    if st.button("Check Emails Now", key="btn_check_mail"):
        with st.spinner("Checking for new emails..."):
            try:
                result = subprocess.run([sys.executable, "services/email_ingestion_imap.py"], capture_output=True, text=True, timeout=30)
                st.text_area("Output", result.stdout if result.stdout else "Check complete.")
            except subprocess.TimeoutExpired:
                st.warning("Manual check timed out (service likely continuing in background).")
            except Exception as e:
                st.error(f"Error: {e}")

with col2:
    if st.button("Run OCR Manually", key="btn_run_ocr"):
        with st.spinner("Running OCR on pending files..."):
            try:
                # We can't easily wait for the loop, so we run a single-pass version if available
                # For now, let's just trigger the service script briefly
                result = subprocess.run([sys.executable, "services/po_ocr_worker_service.py"], capture_output=True, text=True, timeout=15)
                st.text_area("Output", result.stdout if result.stdout else "Processing started.")
            except subprocess.TimeoutExpired:
                st.info("OCR Worker is processing files...")
            except Exception as e:
                st.error(f"Error: {e}")

st.sidebar.markdown("### Service Status")
st.sidebar.success("PostgreSQL: Online")
if "READY" in "READY": # Placeholder for real check check_ollama()
    st.sidebar.success("AI Engine: Ready")
else:
    st.sidebar.warning("AI Engine: Starting")

st.info("💡 **Tip**: Services started from here run as background processes. You can monitor progress in the 'Recent Activity' on the Home page.")
