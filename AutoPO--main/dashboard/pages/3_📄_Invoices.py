import streamlit as st
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dashboard.utils.theme import apply_theme, display_logo
from dashboard.utils.file_utils import get_invoice_list
import pandas as pd

# Page config
st.set_page_config(
    page_title="Invoices - Involexis",
    page_icon="📄",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

st.title("📄 Generated Invoices")

try:
    invoices = get_invoice_list()
    
    if invoices:
        # Convert to DataFrame
        df = pd.DataFrame(invoices)
        df['size_kb'] = df['size'] / 1024
        
        st.markdown(f"**Total Invoices: {len(invoices)}**")
        
        # Display table
        st.dataframe(
            df[['filename', 'created', 'size_kb']].rename(columns={
                'filename': 'Invoice File',
                'created': 'Generated Date',
                'size_kb': 'Size (KB)'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Download section
        st.markdown("---")
        st.subheader("📥 Download Invoice")
        
        selected_invoice = st.selectbox(
            "Select invoice to download",
            options=[inv['filename'] for inv in invoices]
        )
        
        if selected_invoice:
            invoice_data = next(inv for inv in invoices if inv['filename'] == selected_invoice)
            
            with open(invoice_data['path'], 'rb') as f:
                st.download_button(
                    label=f"Download {selected_invoice}",
                    data=f,
                    file_name=selected_invoice,
                    mime='application/pdf'
                )
    
    else:
        st.info("No invoices found. Invoices will appear here after POs are processed.")

except Exception as e:
    st.error(f"Error loading invoices: {str(e)}")
