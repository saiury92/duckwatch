# Lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps for pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m appuser
WORKDIR /app

# Install Python deps first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY ./ ./

# Default env (can be overridden in compose)
ENV WATCH_DIR=/app/data/watched_folder \
    PARQUET_DIR=/app/data/parquet_out \
    DUCKDB_DB=/app/data/db/mydb.duckdb

# Run as non-root
USER appuser

# Entry
CMD ["python", "-u", "watch_convert_duckdb.py"]
