FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        gcc \
        g++ \
        make \
    ; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DB_HOST=db \
    DB_PORT=5433 \
    DB_NAME=csv_db \
    DB_USER=csv_user \
    DB_PASSWORD=csv_password \
    LOG_DIR=logs \
    VIEWS_SQL_PATH=sql/views.sql \
    SPARK_APP_NAME=csv_loader \
    SPARK_MASTER="local[*]" \
    USERS_CSV_PATH=/app/data/users_big.csv \
    PRODUCTS_CSV_PATH=/app/data/products_big.csv \
    ORDERS_CSV_PATH=/app/data/orders_big.csv

CMD ["python", "main.py", "--init-db", "--create-views"]
