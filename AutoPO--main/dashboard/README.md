# Involexis Dashboard

A Streamlit-based dashboard for monitoring the automated Purchase Order pipeline.

## Features

### 📊 Home Dashboard
- Total PO count and status breakdown
- Top selling products (current month)
- Email activity metrics
- Recent activity timeline

### 📦 Purchase Orders
- **Complete list of all POs** with filtering
- Filter by status, buyer, or PO number
- Detailed PO view with line items
- Raw JSON data viewer

### 📄 Invoices
- List of all generated invoices
- Download invoices as PDF
- Invoice metadata (date, size)

### 📧 Email Monitoring
- Track emails processed
- Email activity timeline
- Status of email responses

### 📋 JSON Files
- Browse all processed JSON files
- Formatted and raw JSON views
- Download JSON data

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure PostgreSQL database is running

## Running the Dashboard

From the project root directory:

```bash
streamlit run dashboard/Home.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Theme

The dashboard uses Involexis branding with:
- **Primary Color**: Crimson Red (#DC143C)
- **Background**: Dark theme (#1E1E1E)
- **Accent**: Black and red color scheme

## Pages

1. **Home** - Analytics overview
2. **Purchase Orders** - All POs with search and filter
3. **Invoices** - Generated invoice tracking
4. **Emails** - Email monitoring
5. **JSON Files** - Processed data viewer

## Database Connection

The dashboard connects to PostgreSQL using credentials from `config/db_config.py`. Ensure your database is accessible before running.
