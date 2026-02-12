import streamlit as st
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dashboard.utils.theme import apply_theme, display_logo
from dashboard.utils.file_utils import get_json_files, read_json_file
import pandas as pd

# Page config
st.set_page_config(
    page_title="JSON Files - Involexis",
    page_icon="📋",
    layout="wide"
)

# Apply theme
apply_theme()
display_logo()

st.title("📋 Processed JSON Files")

try:
    json_files = get_json_files()
    
    if json_files:
        # Convert to DataFrame
        df = pd.DataFrame(json_files)
        df['size_kb'] = df['size'] / 1024
        
        st.markdown(f"**Total JSON Files: {len(json_files)}**")
        
        # Display table
        st.dataframe(
            df[['filename', 'created', 'size_kb']].rename(columns={
                'filename': 'JSON File',
                'created': 'Processed Date',
                'size_kb': 'Size (KB)'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # JSON Viewer
        st.markdown("---")
        st.subheader("🔍 JSON Viewer")
        
        selected_file = st.selectbox(
            "Select JSON file to view",
            options=[f['filename'] for f in json_files]
        )
        
        if selected_file:
            file_data = next(f for f in json_files if f['filename'] == selected_file)
            
            try:
                json_content = read_json_file(file_data['path'])
                
                # Display tabs for different views
                tab1, tab2 = st.tabs(["📄 Formatted View", "💾 Raw JSON"])
                
                with tab1:
                    # Extracted data
                    if 'extracted_data' in json_content:
                        st.subheader("Extracted Data")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Buyer Information**")
                            if 'buyer' in json_content['extracted_data']:
                                st.json(json_content['extracted_data']['buyer'])
                        
                        with col2:
                            st.markdown("**Seller Information**")
                            if 'seller' in json_content['extracted_data']:
                                st.json(json_content['extracted_data']['seller'])
                        
                        st.markdown("**Line Items**")
                        if 'line_items' in json_content['extracted_data']:
                            items_df = pd.DataFrame(json_content['extracted_data']['line_items'])
                            st.dataframe(items_df, use_container_width=True, hide_index=True)
                    
                    # Email metadata
                    if 'email_metadata' in json_content:
                        st.markdown("---")
                        st.subheader("Email Metadata")
                        st.json(json_content['email_metadata'])
                
                with tab2:
                    st.json(json_content)
                
                # Download button
                st.download_button(
                    label=f"Download {selected_file}",
                    data=str(json_content),
                    file_name=selected_file,
                    mime='application/json'
                )
            
            except Exception as e:
                st.error(f"Error reading JSON file: {str(e)}")
    
    else:
        st.info("No JSON files found. Files will appear here after POs are processed.")

except Exception as e:
    st.error(f"Error loading JSON files: {str(e)}")
