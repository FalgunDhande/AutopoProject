"""
Professional Data Loader - New Inventory Dataset
Loads cleaned inventory data into PostgreSQL
Author: Yash Rajendra Patil
Date: January 21, 2026
"""

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sys

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'inventory_db',  # New database name
    'user': 'postgres',
    'password': 'root@123'
}

# Path to your new dataset
EXCEL_FILE = r'C:\Users\Yash rajendra patil\Downloads\flipkart dataset.xlsx'

# ============================================================
# FUNCTIONS
# ============================================================

def create_connection_string(config):
    """Create PostgreSQL connection string with URL encoding"""
    encoded_password = quote_plus(config['password'])
    return (
        f"postgresql://{config['user']}:{encoded_password}@"
        f"{config['host']}:{config['port']}/{config['database']}"
    )


def load_and_clean_data(file_path):
    """Load Excel and prepare for database insertion"""
    
    print(f"📂 Loading data from: {file_path}\n")
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        print(f"✅ Loaded {len(df)} rows")
        print(f"📊 Columns: {list(df.columns)}\n")
        
        # Clean column names (remove spaces, lowercase)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Convert date columns
        if 'listing_date' in df.columns:
            df['listing_date'] = pd.to_datetime(df['listing_date'], errors='coerce').dt.date
        
        # Handle missing values
        df = df.fillna({
            'category': 'Uncategorized',
            'stock_available': 0,
            'units_sold': 0,
            'delivery_days': 7
        })
        
        # Ensure correct data types
        df['stock_available'] = df['stock_available'].astype(int)
        df['units_sold'] = df['units_sold'].astype(int)
        df['price'] = df['price'].astype(float)
        
        # Select only required columns
        required_cols = [
            'product_id', 'product_name', 'category', 'price',
            'stock_available', 'units_sold', 'listing_date', 'delivery_days'
        ]
        
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
        
        # Load data
        df.to_sql(
            name='inventory',
            con=engine,
            if_exists='replace',  # Replace existing data
            index=False,
            method='multi'
        )
        
        print(f"✅ Successfully loaded {len(df)} products into database!")
        
        # Verification
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM inventory")
            count = result.fetchone()[0]
            print(f"✅ Verification: {count} products in database\n")
            
            # Show sample
            result = conn.execute("SELECT * FROM inventory LIMIT 3")
            print("Sample from database:")
            print("="*80)
            for row in result:
                print(row)
            print("="*80)
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


def main():
    """Main execution"""
    
    print("="*80)
    print("  PROFESSIONAL INVENTORY DATA LOADER")
    print("="*80 + "\n")
    
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
        print("\nNext steps:")
        print("  1. Open pgAdmin")
        print("  2. View inventory table data")
        print("  3. Proceed to matching pipeline implementation")
    else:
        print("\n❌ Data loading failed")


if __name__ == "__main__":
    main()