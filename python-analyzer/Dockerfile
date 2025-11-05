# ===========================
# Stage 1: Base setup
# ===========================
FROM python:3.11-slim AS base

WORKDIR /app

# System dependencies (Tesseract, zbar for QR/OCR)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tesseract-ocr \
        libzbar0 \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies (cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# ===========================
# Stage 2: Development (reloadable)
# ===========================
FROM base AS dev
WORKDIR /app
COPY . .

ENV PYTHONUNBUFFERED=1
ENV WATCHFILES_FORCE_POLLING=true

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
