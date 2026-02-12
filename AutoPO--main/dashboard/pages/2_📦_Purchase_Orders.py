import streamlit as st
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dashboard.utils.theme import apply_theme, display_logo
from dashboard.utils.db_queries import get_all_pos, get_po_details
import json

# Page config
st.set_page_config(
    page_title="Purchase Orders - Involexis",
    page_icon="📦",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

st.title("📦 Purchase Orders")

try:
    # Get all POs
    pos_df = get_all_pos()
    
    if not pos_df.empty:
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=pos_df['status'].unique().tolist(),
                default=pos_df['status'].unique().tolist()
            )
        
        with col2:
            buyer_filter = st.multiselect(
                "Filter by Buyer",
                options=pos_df['buyer'].dropna().unique().tolist(),
                default=pos_df['buyer'].dropna().unique().tolist()
            )
        
        with col3:
            search = st.text_input("Search PO Number")
        
        # Apply filters
        filtered_df = pos_df[pos_df['status'].isin(status_filter)]
        filtered_df = filtered_df[filtered_df['buyer'].isin(buyer_filter)]
        
        if search:
            filtered_df = filtered_df[filtered_df['po_number'].str.contains(search, case=False, na=False)]
        
        # Display summary
        st.markdown(f"**Showing {len(filtered_df)} of {len(pos_df)} purchase orders**")
        
        # Display table
        st.dataframe(
            filtered_df[[
                'po_number', 'buyer', 'supplier', 'total_amount', 
                'status', 'po_date', 'created_at'
            ]],
            use_container_width=True,
            hide_index=True
        )
        
        # PO Details section
        st.markdown("---")
        st.subheader("📋 PO Details")
        
        selected_po = st.selectbox(
            "Select PO to view details",
            options=filtered_df['po_id'].tolist(),
            format_func=lambda x: filtered_df[filtered_df['po_id'] == x]['po_number'].iloc[0]
        )
        
        if selected_po:
            header_df, items_df = get_po_details(selected_po)
            
            if not header_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Header Information**")
                    st.json({
                        "PO Number": str(header_df['po_number'].iloc[0]),
                        "Buyer": str(header_df['buyer'].iloc[0]),
                        "Supplier": str(header_df['supplier'].iloc[0]),
                        "Total Amount": float(header_df['total_amount'].iloc[0]) if header_df['total_amount'].iloc[0] else 0,
                        "Status": str(header_df['status'].iloc[0]),
                        "Email": str(header_df['sender_email'].iloc[0]) if header_df['sender_email'].iloc[0] else "N/A"
                    })
                
                with col2:
                    st.markdown("**Line Items**")
                    if not items_df.empty:
                        st.dataframe(
                            items_df[[
                                'product_id', 'product_name', 'quantity', 
                                'unit_price', 'line_total'
                            ]],
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info("No line items found")
                
                # Raw JSON
                if header_df['raw_json'].iloc[0]:
                    with st.expander("View Raw JSON"):
                        try:
                            raw_json = json.loads(header_df['raw_json'].iloc[0])
                            st.json(raw_json)
                        except:
                            st.text(header_df['raw_json'].iloc[0])
    
    else:
        st.info("No purchase orders found in the database.")

except Exception as e:
    st.error(f"Error loading purchase orders: {str(e)}")
