"""
Involexis Dashboard Theme Configuration
Modern Blue, White, and Soft Gray palette
"""

# Color palette - Professional Development Blue
PRIMARY_COLOR = "#1B4965"  # Navy/Royal Blue
SECONDARY_COLOR = "#F8F9FA"  # Soft Gray
BACKGROUND_COLOR = "#FFFFFF"  # White
TEXT_COLOR = "#2D3436"  # Dark Gray
ACCENT_COLOR = "#2A9D8F"  # Teal Accent

# Custom CSS for "Modern Professional" look
CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    html, body, [data-testid="stAppViewContainer"] {
        font-family: 'Inter', sans-serif;
        background-color: #F8F9FA;
    }

    .stApp {
        background-color: #F8F9FA;
        color: #2D3436 !important;
    }
    
    [data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E9ECEF;
        box-shadow: 2px 0 5px rgba(0,0,0,0.02);
    }
    
    [data-testid="stSidebar"] * {
        color: #1B4965 !important;
    }
    
    h1, h2, h3 {
        color: #1B4965 !important;
        font-weight: 700 !important;
    }
    
    .stMetric {
        background-color: #FFFFFF;
        padding: 24px;
        border-radius: 12px;
        border: 1px solid #E9ECEF;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    
    .stMetric label {
        color: #636E72 !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stMetric div {
        color: #1B4965 !important;
        font-weight: 700 !important;
        font-size: 1.8rem !important;
    }
    
    .stDataFrame {
        background-color: #FFFFFF;
        border-radius: 12px;
        border: 1px solid #E9ECEF;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05);
    }
    
    .stButton>button {
        background-color: #1B4965;
        color: white;
        border-radius: 8px;
        border: none;
        padding: 10px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(27, 73, 101, 0.2);
    }
    
    .stButton>button:hover {
        background-color: #215A7A;
        transform: translateY(-1px);
        box-shadow: 0 6px 12px rgba(27, 73, 101, 0.3);
        color: white;
    }
    
    /* Logo styling */
    .logo-text {
        font-size: 1.8em;
        font-weight: 800;
        color: #1B4965;
        text-align: center;
        padding: 25px;
        background: #FFFFFF;
        border-bottom: 2px solid #1B4965;
        margin-bottom: 30px;
        letter-spacing: 2px;
    }
</style>
"""

def apply_theme():
    """Apply custom theme to Streamlit app."""
    import streamlit as st
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
def display_logo():
    """Display Involexis logo/branding."""
    import streamlit as st
    st.markdown('<div class="logo-text">INVOLEXIS</div>', unsafe_allow_html=True)



