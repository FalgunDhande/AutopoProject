import streamlit as st
import sys
import os

# ----------------------------------------------------------------
# DEPENDENCY CHECK
# ----------------------------------------------------------------
try:
    import psycopg2
    import pandas as pd
    import plotly
except ImportError as e:
    st.error(f"❌ Missing Dependency: {e}")
    st.info("Please run: `pip install psycopg2-binary pandas plotly streamlit` in your virtual environment.")
    st.stop()

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_queries import get_po_summary, get_monthly_sales, get_email_count, get_recent_activity, get_db_connection
from utils.theme import apply_theme, display_logo
import plotly.express as px
import plotly.graph_objects as go
import requests

def check_ollama():
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=2)
        return response.status_code == 200
    except:
        return False

# Page config
st.set_page_config(
    page_title="Involexis | Business Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply theme
apply_theme()

# Sidebar Branding
with st.sidebar:
    display_logo()
    st.markdown("---")
    st.markdown("### 🛠️ System Status")
    
    # DB Status
    try:
        conn = get_db_connection()
        conn.close()
        st.success("Database: Connected")
    except:
        st.error("Database: Offline")
        
    # Ollama Status
    if check_ollama():
        st.success("AI Engine (Ollama): Online")
    else:
        st.warning("AI Engine (Ollama): Offline")
        st.info("💡 Start Ollama to enable OCR.")
        
    st.success("Email Service: Ready")

    st.markdown("---")
    st.info("Involexis Professional Edition")

# Main Header
st.title("📊 Enterprise Dashboard")
st.markdown("Automated Purchase Order Analytics & Pipeline Management")
st.markdown("---")

# Metrics row
col1, col2, col3, col4 = st.columns(4)

try:
    # Get data
    po_summary = get_po_summary()
    email_count = get_email_count()
    monthly_sales = get_monthly_sales()
    
    with col1:
        total_pos = po_summary['total_pos'].iloc[0] if not po_summary.empty else 0
        st.metric("📦 Total Orders", total_pos, "Monthly Growth")
    
    with col2:
        completed = po_summary['completed'].iloc[0] if not po_summary.empty else 0
        st.metric("✅ Competed", completed)
    
    with col3:
        st.metric("📧 Total Emails", email_count)
    
    with col4:
        pending = po_summary['pending'].iloc[0] if not po_summary.empty else 0
        st.metric("⏳ Pending Action", pending)

    
    st.markdown("---")

    # Center Row: Insights and Activity
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("📈 Top Performing Products")
        if not monthly_sales.empty:
            # Modern Navy & Teal charts
            fig = px.bar(
                monthly_sales,
                x='product_name',
                y='total_quantity',
                text='total_quantity',
                color='total_quantity',
                color_continuous_scale=['#2A9D8F', '#1B4965'],
                labels={'total_quantity': 'Units Sold', 'product_name': 'Product'}
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#1B4965',
                showlegend=False,
                margin=dict(t=20, b=20, l=20, r=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sales data available.")

    with col_right:
        st.subheader("📊 Status Distribution")
        if not po_summary.empty:
            status_data = {
                'Status': ['Completed', 'Partial', 'Pending', 'Failed'],
                'Count': [
                    po_summary['completed'].iloc[0],
                    po_summary['partial'].iloc[0],
                    po_summary['pending'].iloc[0],
                    po_summary['failed'].iloc[0]
                ]
            }
            fig = px.pie(
                status_data,
                values='Count',
                names='Status',
                hole=0.4,
                color_discrete_sequence=['#1B4965', '#2A9D8F', '#E9ECEF', '#CED4DA']
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#1B4965',
                margin=dict(t=20, b=20, l=20, r=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Bottom Row: Recent Transactions
    st.subheader("🕒 Live Activity Feed")
    recent = get_recent_activity(15)
    if not recent.empty:
        # Style the dataframe for light theme
        def color_status(val):
            if val == 'COMPLETED': return 'color: #0F5132; background-color: #D1E7DD; border-radius: 4px;'
            if val == 'FAILED': return 'color: #842029; background-color: #F8D7DA; border-radius: 4px;'
            if 'WAITING' in str(val): return 'color: #664D03; background-color: #FFF3CD; border-radius: 4px;'
            return 'color: #1B4965'

        st.dataframe(
            recent.style.map(color_status, subset=['status']),
            use_container_width=True,
            hide_index=True
        )


    else:
        st.info("No incoming activity detected in the last cycle.")

except Exception as e:
    st.error(f"⚠️ Analytics Engine Error: {str(e)}")
    st.warning("Ensure the PostgreSQL database is online and 'schema.sql' has been fully applied.")

    st.info("Involexis Enterprise v2.5")

# Footer
st.markdown("---")
st.markdown(
    '<p style="text-align: center; color: #1D3557; opacity: 0.6;">Involexis Enterprise PO Platform | Intelligence & Automation</p>', 
    unsafe_allow_html=True
)

