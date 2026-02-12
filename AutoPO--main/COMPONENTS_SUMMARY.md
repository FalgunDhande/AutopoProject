# Project Components & LLM Agents Summary

This document provides a comprehensive overview of the tools, technologies, and LLM-driven agents that power the Automated Purchase Order (PO) Pipeline.

## 🏗️ Architecture Overview
The system is built as a microservices architecture using **Docker Compose**, ensuring scalability and isolation for the database, AI models, and processing workers.

## 🤖 LLM Agents & AI Integration
The pipeline uses **local LLMs** via **Ollama** for privacy and efficiency.

| Agent Name | Component | LLM Model | Primary Role |
| :--- | :--- | :--- | :--- |
| **Extraction Agent** | `core/po_ocr_worker.py` | `qwen2.5:7b` | Extracts structured JSON data from OCR text. |
| **Communication Agent**| `core/optimized_agent.py` | `qwen2.5:1.5b` | Generates professional, context-aware email bodies. |
| **Decision Engine** | `core/optimized_agent.py` | Logic-based | Analyzes inventory to decide on Full/Partial/None fulfillment. |

## 🛠️ Technology Stack

### Core Technologies
- **Language**: Python 3.10+
- **Database**: PostgreSQL 15 (Managed via SQLAlchemy & Psycopg2)
- **Containerization**: Docker & Docker Compose
- **Orchestration**: Custom shell scripts (`start.sh`, `stop.sh`)

### Major Libraries & Frameworks
- **Web Dashboard**: Flask, CSS (Vanilla), HTML/Jinja2.
- **Data Analysis**: Pandas, NumPy, Openpyxl.
- **OCR & PDF Processing**:
    - **PaddleOCR**: High-accuracy text extraction.
    - **pdfplumber / pdf2image**: PDF parsing and conversion.
    - **ReportLab**: Dynamic PDF invoice generation.
- **Automation**: `schedule` for periodic ML & background tasks.
- **Email**: Python `smtplib` & `imaplib` for ingestion and communication.

## 📂 Service Breakdown
1. **`postgres`**: Centralized storage for POs, Inventory, and Invoices.
2. **`ollama`**: Local inference server for Qwen2.5 models.
3. **`email-ingestion`**: Monitors IMAP folders for incoming orders.
4. **`ocr-worker`**: Converts raw POs (PDF/Images) into structured data.
5. **`flask-app`**: Enterprise dashboard for monitoring and control.
6. **`reply-listener`**: Handles customer approval for partial shipments.
7. **`scheduler`**: Manages demand forecasting and system maintenance.

---
*Generated: 2026-02-11*
