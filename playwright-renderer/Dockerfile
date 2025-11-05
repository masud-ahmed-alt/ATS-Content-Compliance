FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    mkdir -p /app/cli/screenshots && chmod -R 777 /app/cli/screenshots

COPY . .

EXPOSE 9000

ENV PYTHONUNBUFFERED=1
ENV WATCHFILES_FORCE_POLLING=true

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000", "--reload"]
