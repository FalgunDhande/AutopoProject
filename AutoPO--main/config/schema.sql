
-- Tables for PO Pipeline

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id SERIAL PRIMARY KEY,
    po_number VARCHAR(100),
    po_date DATE,
    buyer VARCHAR(255),
    supplier VARCHAR(255),
    buyer_gst VARCHAR(50),
    supplier_gst VARCHAR(50),
    currency VARCHAR(10),
    total_amount NUMERIC(15, 2),
    status VARCHAR(50) DEFAULT 'NEW',
    raw_json TEXT,
    sender_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchase_order_items (
    item_id SERIAL PRIMARY KEY,
    po_id INT REFERENCES purchase_orders(po_id) ON DELETE CASCADE,
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    quantity INT,
    unit_price NUMERIC(15, 2),
    line_total NUMERIC(15, 2)
);

-- Note: 'inventory' table is created by load_data.py
