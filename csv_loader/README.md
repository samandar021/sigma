# CSV Loader — PySpark + PostgreSQL ETL Pipeline (Dockerized)

This project implements an end-to-end ETL pipeline using **PySpark**, **PostgreSQL**, and **SQLAlchemy**, packaged in a fully reproducible **Docker** environment.  
The system validates CSV input data, loads the cleaned dataset into a relational database, and generates analytical views for downstream reporting.

---

## 1. Architecture Overview

### 1.1 High-Level Data Flow

CSV Files
│
▼
PySpark Validation Pipeline
│
▼
SQLAlchemy ORM Loader
│
▼
PostgreSQL Database
│
▼
Analytical SQL Views

markdown
Copy code

### 1.2 Component Responsibilities

| Module | Responsibility |
|--------|----------------|
| `app/config.py`       | Environment-driven config (DB, CSV paths, Spark settings). |
| `app/db.py`           | DB engine creation, schema init, view execution. |
| `app/models.py`       | SQLAlchemy ORM models (`User`, `Product`, `Order`). |
| `app/spark_job.py`    | Spark session, CSV parsing with schemas, validation rules. |
| `app/loader.py`       | Converts Spark rows → ORM entities; batch-inserts into DB. |
| `app/logging_conf.py` | Logging (stdout + `/app/logs/app.log`). |
| `main.py`             | Orchestration of full ETL run. |
| `sql/views.sql`       | SQL definitions for analytics views. |
| `tests/`              | Automated tests using Spark + SQLite. |

---

## 2. Data Schema & Validation

### Input CSV Structure

- **users.csv** → `id, name, email`
- **products.csv** → `id, name, price`
- **orders.csv** → `id, user_id, product_id, quantity`

### Validation Logic (Spark)

| Rule | Description |
|------|-------------|
| `email` | Must match valid email regex |
| `price` | Must be strictly greater than `0` |
| `quantity` | Must be `≥ 1` |
| FK validation | `user_id` must exist in users, `product_id` must exist in products |

Invalid rows are excluded from database loading.

---

## 3. SQL Analytical Views

Defined in `sql/views.sql`.

### 3.1 `users_extended`
Includes:
- `total_spent` (SUM(quantity × price))
- `order_count`
- `avg_spent` (AVG(quantity × price))

### 3.2 `orders_extended`
Includes:
- `user_name`
- `product_name`
- `total_price_per_order` (quantity × price)

---

## 4. Project Structure

csv_loader/
├── app/
│ ├── config.py
│ ├── db.py
│ ├── models.py
│ ├── loader.py
│ ├── spark_job.py
│ ├── logging_conf.py
│ └── init.py
│
├── data/ # CSV files mounted into Docker
├── logs/ # Runtime logs
├── sql/
│ └── views.sql # SQL views
│
├── tests/
│ ├── conftest.py
│ └── test_loader.py
│
├── Dockerfile
├── docker-compose.yml
└── README.md

yaml
Copy code

---

## 5. Dockerized Execution

The application is fully containerized.  
To run the ETL pipeline:

### 5.1 Build & Start the Services

```bash
docker-compose up --build
What happens:
PostgreSQL starts and becomes healthy.

ETL app container starts.

Application:

Loads configuration from environment

Initializes DB schema (--init-db)

Creates Spark session

Reads CSVs from /app/data

Validates data using Spark

Loads clean data into PostgreSQL

Creates SQL views (--create-views)

Stops (exit code 0)

To follow the logs:

bash
Copy code
docker logs -f csv_loader_app
Application logs also persist in:

bash
Copy code
logs/app.log
6. Dockerfile (used by the project)
dockerfile
Copy code
FROM python:3.11-slim

# Install Java for PySpark + minimal build tools
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

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Default ENV for Spark + DB
ENV SPARK_APP_NAME=csv_loader \
    SPARK_MASTER=local[*] \
    LOG_DIR=/app/logs \
    USERS_CSV_PATH=/app/data/users_big.csv \
    PRODUCTS_CSV_PATH=/app/data/products_big.csv \
    ORDERS_CSV_PATH=/app/data/orders_big.csv \
    VIEWS_SQL_PATH=/app/sql/views.sql \
    DB_HOST=db \
    DB_PORT=5432 \
    DB_NAME=csv_db \
    DB_USER=csv_user \
    DB_PASSWORD=csv_password

CMD [
  "python", "main.py",
  "--users", "/app/data/users_big.csv",
  "--products", "/app/data/products_big.csv",
  "--orders", "/app/data/orders_big.csv",
  "--init-db",
  "--create-views"
]
7. docker-compose.yml (included in repository)
yaml
Copy code
services:
  db:
    image: postgres:15
    container_name: csv_loader_db
    environment:
      POSTGRES_USER: csv_user
      POSTGRES_PASSWORD: csv_password
      POSTGRES_DB: csv_db
    ports:
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U csv_user -d csv_db"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build: .
    container_name: csv_loader_app
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./sql:/app/sql

volumes:
  pgdata:
8. Configuration (Environment Variables)
Variable	Default	Description
DB_HOST	db	PostgreSQL hostname
DB_PORT	5432	Database port
DB_NAME	csv_db	Database name
DB_USER	csv_user	Username
DB_PASSWORD	csv_password	Password
USERS_CSV_PATH	/app/data/users_big.csv	Users CSV
PRODUCTS_CSV_PATH	/app/data/products_big.csv	Products CSV
ORDERS_CSV_PATH	/app/data/orders_big.csv	Orders CSV
VIEWS_SQL_PATH	/app/sql/views.sql	SQL view definitions
SPARK_APP_NAME	csv_loader	Spark application name
SPARK_MASTER	local[*]	Spark master URL
LOG_DIR	/app/logs	Log output directory

9. Manual CLI Usage
Inside container or locally:

bash
Copy code
python main.py \
    --users data/users_big.csv \
    --products data/products_big.csv \
    --orders data/orders_big.csv \
    --init-db \
    --create-views
10. Tests
The test suite uses:

Temporary SQLite DB

Local Spark session

SQLAlchemy ORM

Run tests:

bash
Copy code
pytest -q
Tests validate:

Spark → ORM → DB load path

Price/quantity/email rules

Foreign key validation

Decimal handling for product pricing

11. Execution Flow Summary
arduino
Copy code
main.py
 │
 ├── setup_logging()
 ├── parse_args()
 ├── config = AppConfig.from_env()
 ├── db.init_db()
 ├── spark = create_spark_session()
 ├── df_users, df_products, df_orders = read_csvs()
 ├── validate_users()
 ├── validate_products()
 ├── validate_orders()
 ├── load_users()
 ├── load_products()
 ├── load_orders()
 ├── db.create_views()
 └── exit(0)
12. Notes
The structure separates Spark logic, DB logic, and data loading for testability.

Docker ensures environment reproducibility.

All logging is centralized and timestamped.

Spark schemas ensure strongly typed input validation.

SQL views enable further analytics without modifying ETL logic.

13. License
MIT License (or specify as needed).

yaml
Copy code

---

If you want, I can also add:

✅ Mermaid architecture diagrams  
✅ A short “How to deploy on AWS ECS/K8s” section  
✅ A Yandex-style engineering justification  

Just tell me.