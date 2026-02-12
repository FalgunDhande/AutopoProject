#!/bin/bash

# Involexis PO Pipeline - Package for Sharing Script
# This script creates a clean ZIP file of the project that you can send to anyone.

PROJECT_NAME="po_pipeline_shared"
ZIP_NAME="${PROJECT_NAME}.zip"

echo "📦 Packaging project for sharing..."

# 1. Check if zip command exists
if ! command -v zip &> /dev/null; then
    echo "❌ 'zip' command not found. Please install zip utility or zip the folder manually."
    exit 1
fi

# 2. Remove old zip if it exists
rm -f "$ZIP_NAME"

# 3. Create a clean ZIP
# We exclude:
# - .env (Secrets!)
# - postgres-data (Huge database folder)
# - .git (Version history)
# - __pycache__, .pytest_cache (Python caches)
# - venv, .venv (Virtual environments)
# - .DS_Store (Mac noise)

echo "🤐 Zipping files (this excludes your passwords and heavy data)..."

zip -r "$ZIP_NAME" . \
    -x "*.env*" \
    -x "postgres-data/*" \
    -x ".git/*" \
    -x "**/__pycache__/*" \
    -x "**/__pycache__" \
    -x ".venv/*" \
    -x ".venv" \
    -x "venv/*" \
    -x "venv" \
    -x ".DS_Store" \
    -x "ollama-data/*" \
    -x "node_modules/*" \
    -x "package-for-sharing.sh"

if [ $? -eq 0 ]; then
    echo "------------------------------------------------"
    echo "✅ SUCCESS! Your project is packaged."
    echo "📂 File created: $ZIP_NAME"
    echo "------------------------------------------------"
    echo "🚀 HOW TO SHARE:"
    echo "1. Send this '$ZIP_NAME' file to the other person."
    echo "2. Tell them to follow the steps in 'DOCKER_QUICKSTART.md'."
    echo "3. Remember: They will need to create their own .env file."
    echo "------------------------------------------------"
else
    echo "❌ Failed to create ZIP file."
    exit 1
fi
