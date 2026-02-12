import streamlit as st
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dashboard.utils.theme import apply_theme, display_logo
from dashboard.utils.db_queries import get_all_pos
import pandas as pd

# Page config
st.set_page_config(
    page_title="Emails - Involexis",
    page_icon="📧",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

st.title("📧 Email Monitoring")

try:
    # Get POs with email data
    pos_df = get_all_pos()
    
    if not pos_df.empty:
        # Filter POs with emails
        email_df = pos_df[pos_df['sender_email'].notna()].copy()
        
        st.markdown(f"**Total Emails Processed: {len(email_df)}**")
        
        # Email statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Emails Received", len(email_df))
        
        with col2:
            completed_emails = len(email_df[email_df['status'].isin(['COMPLETED', 'PARTIAL_COMPLETED'])])
            st.metric("Invoices Sent", completed_emails)
        
        with col3:
            pending_emails = len(email_df[email_df['status'] == 'WAITING_FOR_REPLY'])
            st.metric("Awaiting Reply", pending_emails)
        
        # Email list
        st.markdown("---")
        st.subheader("📬 Email History")
        
        st.dataframe(
            email_df[[
                'po_number', 'sender_email', 'buyer', 
                'status', 'created_at'
            ]].rename(columns={
                'po_number': 'PO Number',
                'sender_email': 'From Email',
                'buyer': 'Buyer',
                'status': 'Status',
                'created_at': 'Received At'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Email timeline
        st.markdown("---")
        st.subheader("📊 Email Activity Timeline")
        
        email_df['date'] = pd.to_datetime(email_df['created_at']).dt.date
        daily_counts = email_df.groupby('date').size().reset_index(name='count')
        
        st.line_chart(daily_counts.set_index('date'))
    
    else:
        st.info("No email data available.")

except Exception as e:
    st.error(f"Error loading email data: {str(e)}")

st.markdown("---")
st.info("💡 **Note**: Email monitoring shows POs received via email. For full Gmail integration, configure Gmail API credentials.")
