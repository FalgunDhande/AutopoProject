# 🐳 Docker Quick Start Guide

## Prerequisites

1. **Install Docker Desktop** (if not already installed)
   - Download from: https://www.docker.com/products/docker-desktop/
   - For Mac with Apple Silicon (M1/M2/M3): Choose "Mac with Apple chip"
   - For Intel Mac: Choose "Mac with Intel chip"
   - Install and launch Docker Desktop

2. **Verify Installation**
   ```bash
   docker --version
   docker-compose --version
   ```

## Quick Deployment (3 Steps)

### Step 1: Configure Environment Variables

```bash
# Make sure you're in the project directory
cd /Users/garvitgupta/Desktop/po_pipeline

# Your .env file should already exist with your credentials
# Verify it has the required variables:
cat .env
```

### Step 2: Build and Start Services

```bash
# Build all Docker images (first time only, takes 5-10 minutes)
docker-compose build

# Start all services in the background
docker-compose up -d
``

### Step 3: Initialize Ollama Model

```bash
# Wait for Ollama to be ready (check with: docker-compose ps)
# Then pull the Qwen model (takes 5-10 minutes, ~4GB download)
docker exec po_ollama ollama pull qwen2.5:7b

# Verify the model is loaded
docker exec po_ollama ollama list
```

### Step 4: Initialize Inventory Data (Required for first run)

```bash
# Load the initial inventory data from dataset.xlsx
docker exec po_flask python scripts/load_data.py
```

## Access Your Application

- **Flask Dashboard**: http://localhost:5001
- **PostgreSQL**: localhost:5432
- **Ollama API**: http://localhost:11434

## Common Commands

```bash
# View all running containers
docker-compose ps

# View logs from all services
docker-compose logs -f

# View logs from specific service
docker-compose logs -f flask-app
docker-compose logs -f ocr-worker

# Restart a service
docker-compose restart flask-app

# Stop all services
docker-compose down

# Stop and remove all data (including database)
docker-compose down -v
```

## Troubleshooting

### Services won't start
```bash
# Check if ports are already in use
lsof -i :5001
lsof -i :5432
lsof -i :11434

# Check Docker Desktop is running
docker info
```

### Database connection issues
```bash
# Check if PostgreSQL is healthy
docker-compose ps postgres

# Access database directly
docker exec -it po_postgres psql -U postgres -d po_db
```

### Ollama model not working
```bash
# Check Ollama service
docker-compose logs ollama

# Re-pull the model
docker exec po_ollama ollama pull qwen2.5:7b
```

## Next Steps

1. ✅ All services running
2. ✅ Ollama model loaded
3. 📧 Send test email with PO attachment to test the pipeline
4. 📊 Monitor processing in the Flask dashboard
5. 🔍 Check logs: `docker-compose logs -f`

## 🤝 How to Share This Project

There are two main ways to share this Dockerized pipeline with anyone:

### Option 1: Share the Source Code (Standard)
This is the easiest way. Any user with Docker installed can run it.
1. **Prepare the files**: Include everything in the project folder **except** the `.env` file (for security) and the `postgres-data` folder.
2. **Transfer**: Upload it to **GitHub** (private or public) or send a **ZIP file**.
3. **The Recipient**:
   - Clones/Unpacks the folder.
   - Creates their own `.env` file (using `.env.example` as a template).
   - Runs `docker-compose up -d`.

### Option 2: Share via Docker Hub (Advanced)
If you want to share pre-built images without sharing the source code:
1. **Push**: You would build and push your images to [Docker Hub](https://hub.docker.com/).
2. **Pull**: The receiver only needs the `docker-compose.yml` file. When they run `docker-compose up`, it will pull the pre-built images from your Docker Hub repository.

### Option 3: Share as a Single File (Docker Save/Load)
If you want to send the entire environment as a single file without using a registry:
1. **Save the image**:
   ```bash
   docker save -o po-pipeline-image.tar po-flask:latest
   ```
2. **Send the file**: Send `po-pipeline-image.tar` (note: these files are very large).
3. **The Recipient loads it**:
   ```bash
   docker load -i po-pipeline-image.tar
   ```

> [!CAUTION]
> **Data vs. Images**: Docker images contain the **code and tools**, but **not the data** (like your database or PDFs). To share the data, you must also share the `postgres-data/` folder or a database dump.

> [!IMPORTANT]
> **Never** share your `.env` file. The other person must set up their own Gmail App Passwords and API keys.

---

## Full Documentation

For detailed documentation, see:
- **DOCKER_DEPLOYMENT.md** - Complete deployment guide
- **README.md** - Project overview
- **SECURITY.md** - Security best practices

---

**Need help?** Check logs with `docker-compose logs -f` or refer to DOCKER_DEPLOYMENT.md for detailed troubleshooting.
