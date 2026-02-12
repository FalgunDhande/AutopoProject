import os

# Unified Database Configuration
# Uses environment variables for Docker/Production
# Falls back to local defaults for development

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "po_db"),
    "user": os.getenv("DB_USER", "garvitgupta"),
    "password": os.getenv("DB_PASS", "")
}