# Automated Purchase Order (PO) Pipelines

This system automates the processing of Purchase Orders received via email. It handles extraction, inventory checking, invoicing, and email communication.

## 📂 Project Structure

*   **`config/`**: Configuration (`db_config.py`, `company_info.json`).
*   **`core/`**: Core logic (`agent`, `invoice_gen`, `ocr_worker`, `db_insert`).
*   **`services/`**: Runnable services (`ingestion`, `ocr_service`, `reply_listener`, `scheduler`).
*   **`ml/`**: Machine Learning models (`demand_season`, `sales_history`).
*   **`scripts/`**: Utility scripts (`load_data`, `test_*`).

## 🚀 Prerequisites

1.  **Python 3.10+**
2.  **PostgreSQL** (Database)
3.  **Ollama** (for AI Models: `ollama pull qwen2.5:7b`)

## 🛠️ Setup

1.  **Install Dependencies**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  **Database Configuration**
    *   Edit **`config/db_config.py`** with your credentials.
    *   **Initialize Schema**:
        ```bash
        ./venv/bin/python scripts/load_data.py
        ```

## 🏃‍♂️ Quick Start

Run the entire pipeline in the background with a single command:

```bash
./start.sh
```

- **Dashboard**: [http://localhost:5001](http://localhost:5001)
- **Logs**: View live logs in the dashboard's Control Center or in `./logs/`.

To stop all services:
```bash
./stop.sh
```

## 📊 Flask Dashboard Features

- **Enterprise Overview**: Real-time metrics for POs, completion rates, and sales.
- **Top Performing Products**: Interactive bar chart showing sales volume.
- **Invoices Repository**: Browse and download generated PDF invoices.
- **Control Center**:
    - Trigger full automation or individual services.
    - Live log streaming via SSE.
    - **SMTP Configuration Test**: Verify email settings and signature.
- **Email Monitor**: Track processed emails and their associated POs.

## 🧪 Testing

*   **Test Full Flow**: `./venv/bin/python scripts/test_agent.py`
*   **Test Email Sending**: `./venv/bin/python scripts/test_email.py`
*   **Test Sender Routing**: `./venv/bin/python scripts/test_sender.py`

## 🧩 Architecture Flow

1.  **Ingestion** (`services/email_ingestion_imap.py`) -> Saves PDF -> `manifest.json`.
2.  **Service** (`services/po_ocr_worker_service.py`) -> Pick up PDF -> Call `core/po_ocr_worker.py`.
3.  **OCR/LLM** -> Extract Data -> Call `core/db_insert.py`.
4.  **Insert** -> Save to DB -> Trigger `core/optimized_agent.py`.
5.  **Agent** -> Check Inventory -> Generate Invoice (`core/invoice_generator.py`) -> Send Email.
