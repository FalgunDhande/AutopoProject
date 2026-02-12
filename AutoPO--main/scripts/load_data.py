
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_CONFIG

# ============================================================
# CONFIGURATION
# ============================================================

# Using the dataset found in the root directory
EXCEL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dataset.xlsx")

# ============================================================
# FUNCTIONS
# ============================================================

def create_connection_string(config):
    """Create PostgreSQL connection string with URL encoding"""
    encoded_password = quote_plus(config['password'])
    return (
        f"postgresql+psycopg2://{config['user']}:{encoded_password}@"
        f"{config['host']}:{config['port']}/{config['dbname']}"
    )


def load_and_clean_data(file_path):
    """Load Excel and prepare for database insertion (Optimized)"""
    
    print(f"📂 Loading data from: {file_path}\n")
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        print(f"✅ Loaded {len(df)} rows")
        
        # Clean column names (remove spaces, lowercase)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Handle missing values
        df = df.fillna({
            'category': 'Uncategorized',
            'stock_available': 0,
            'price': 0.0,
            'units_sold': 0
        })
        
        # Ensure correct data types
        df['stock_available'] = df['stock_available'].astype(int)
        df['price'] = df['price'].astype(float)
        
        # --------------------------------------------------------
        # OPTIMIZATION: Keep only essential columns
        # Dropping: brand, seller_city, listing_date, delivery_date
        # --------------------------------------------------------
        required_cols = [
            'product_id', 
            'product_name', 
            'category', 
            'price',
            'stock_available',
            'units_sold'
        ]
        
        # Select only if they exist in source
        existing_cols = [col for col in required_cols if col in df.columns]
        df_clean = df[existing_cols]
        
        print(f"🧹 Data cleaned: {len(df_clean)} rows, {len(existing_cols)} columns")
        print(f"\nSample data (first 3 rows):")
        print("="*80)
        print(df_clean.head(3))
        print("="*80 + "\n")
        
        return df_clean
    
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)


def load_to_database(df, connection_string):
    """Load DataFrame to PostgreSQL"""
    
    print("📤 Loading data to PostgreSQL...\n")
    
    try:
        engine = create_engine(connection_string)
        
        # Load data to 'inventory' table
        df.to_sql(
            name='inventory',
            con=engine,
            if_exists='replace',  # Replace entire table
            index=False,
            method='multi'
        )
        
        print(f"✅ Successfully loaded {len(df)} products into 'inventory' table!")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


def main():
    """Main execution"""
    
    print("="*80)
    print("  OPTIMIZED DATA LOADER")
    print("="*80 + "\n")
    
    if not os.path.exists(EXCEL_FILE):
        print(f"❌ Error: Dataset not found at {EXCEL_FILE}")
        return

    # Load and clean data
    df = load_and_clean_data(EXCEL_FILE)
    
    # Create connection
    conn_string = create_connection_string(DB_CONFIG)
    
    # Load to database
    success = load_to_database(df, conn_string)
    
    if success:
        print("\n" + "="*80)
        print("  ✅ DATA LOADING COMPLETED SUCCESSFULLY!")
        print("="*80)
    else:
        print("\n❌ Data loading failed")


if __name__ == "__main__":
    main()
