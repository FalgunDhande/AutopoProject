#!/bin/bash

# PO Pipeline Docker Deployment Script

echo "🐳 Starting PO Pipeline Docker Deployment..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker could not be found. Please install Docker Desktop."
    exit 1
fi

echo "📋 Configuration Check..."
if [ ! -f .env ]; then
    echo "⚠️ .env file not found. Please ensure you have configured environment variables."
    read -p "Continue anyway (using defaults)? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "🏗️  Building Docker images..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to initialize..."
sleep 10

echo "🧠 Checking Ollama status..."
# Wait for Ollama
until curl -s -f -o /dev/null "http://localhost:11434/api/tags"; do
    echo "Waiting for Ollama API..."
    sleep 5
done

echo "📥 Pulling Qwen model (this may take a few minutes)..."
docker exec po_ollama ollama pull qwen2.5:7b

echo "💾 Initializing Inventory Data..."
docker exec po_flask python scripts/load_data.py

echo "✅ Deployment Complete!"
echo "----------------------------------------"
echo "📊 Dashboard: http://localhost:5001"
echo "🗄️  Database:  localhost:5432"
echo "🤖 Ollama API: http://localhost:11434"
echo "----------------------------------------"
echo "To stop services: docker-compose down"
