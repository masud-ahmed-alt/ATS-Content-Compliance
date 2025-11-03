#!/usr/bin/env python3
"""
config/settings.py (MinIO-only)

Defines MinIO configuration for screenshot upload and storage.
Used by Playwright Renderer or other standalone services that
save data directly to MinIO.
"""

import os

# ======================================================
# 💾 MINIO CONFIGURATION
# ======================================================

# MinIO endpoint — from docker-compose (port 7000 = API)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:7000")

# MinIO credentials (defined in docker-compose)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin")

# Default bucket to store analyzer screenshots
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "analyzer-data")

# ======================================================
# 🧩 INFO
# ======================================================

