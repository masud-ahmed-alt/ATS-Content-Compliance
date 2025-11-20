# ===========================
# Stage 1: Base setup
# ===========================
FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tesseract-ocr \
        libzbar0 \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies (cached unless requirements.txt changes)
COPY requirements.txt .

# Use BuildKit cache mount for pip (much faster rebuilds)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && pip install -r requirements.txt

# Download spaCy model (en_core_web_sm) if spacy is installed
# This is done after pip install to ensure spacy is available
# The model will be cached in the image to avoid re-downloading
RUN if python -c "import spacy" 2>/dev/null; then \
        python -m spacy download en_core_web_sm || \
        echo "Warning: spaCy model download failed. Model will be downloaded at runtime if needed."; \
    else \
        echo "spaCy not installed, skipping model download."; \
    fi

# ===========================
# Stage 2: Development (hot reload)
# ===========================
FROM base AS dev
WORKDIR /app

# Copy only application code (after dependencies)
COPY . .

ENV PYTHONUNBUFFERED=1
ENV WATCHFILES_FORCE_POLLING=true

EXPOSE 8000

# Use Uvicorn for reloadable dev server
# Add --log-level info for better visibility
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]

