from __future__ import annotations

import argparse
import logging
import os
import sys

from app.config import AppConfig
from app.logging_conf import setup_logging
from app.db import Database
from app.spark_job import (
    create_spark_session,
    read_csvs,
    validate_users,
    validate_products,
    validate_orders,
)
from app.loader import load_users, load_products, load_orders


logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV → PySpark → PostgreSQL ETL loader."
    )
    parser.add_argument("--users", required=True, help="Path to users CSV file")
    parser.add_argument("--products", required=True, help="Path to products CSV file")
    parser.add_argument("--orders", required=True, help="Path to orders CSV file")
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Create database schema before loading data",
    )
    parser.add_argument(
        "--create-views",
        action="store_true",
        help="Create analytical SQL views after loading data",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    # ---------------------------------------------------------
    # 1. Logging (STDOUT + ./logs/app.log)
    # ---------------------------------------------------------
    log_dir = os.getenv("LOG_DIR", "/app/logs")
    setup_logging(log_dir)
    logger.info("CSV Loader starting...")

    # ---------------------------------------------------------
    # 2. Parse CLI arguments
    # ---------------------------------------------------------
    args = parse_args(argv)

    # ---------------------------------------------------------
    # 3. Load configuration from environment
    # ---------------------------------------------------------
    config = AppConfig.from_env()
    config.users_csv = args.users
    config.products_csv = args.products
    config.orders_csv = args.orders

    logger.info("Users CSV: %s", config.users_csv)
    logger.info("Products CSV: %s", config.products_csv)
    logger.info("Orders CSV: %s", config.orders_csv)

    # ---------------------------------------------------------
    # 4. Initialize database
    # ---------------------------------------------------------
    db = Database(config=config)

    if args.init_db:
        logger.info("Initializing database schema...")
        db.init_db()

    # ---------------------------------------------------------
    # 5. Run Spark validation pipeline
    # ---------------------------------------------------------
    logger.info("Creating Spark session...")

    # 🔧 FIX: use env variable instead of missing config.spark_app_name
    spark_app_name = os.getenv("SPARK_APP_NAME", "csv_loader")
    spark = create_spark_session(app_name=spark_app_name)

    logger.info("Reading CSVs with Spark...")
    df_users, df_products, df_orders = read_csvs(
        spark,
        config.users_csv,
        config.products_csv,
        config.orders_csv,
    )

    logger.info("Validating users...")
    validate_users(df_users)

    logger.info("Validating products...")
    validate_products(df_products)

    logger.info("Validating orders...")
    validate_orders(df_orders, df_users, df_products)

    # ---------------------------------------------------------
    # 6. Load validated data into PostgreSQL
    # ---------------------------------------------------------
    logger.info("Loading validated data into PostgreSQL...")

    load_users(df_users, db.engine)
    load_products(df_products, db.engine)
    load_orders(df_orders, db.engine)

    logger.info("Data successfully loaded into database")

    # ---------------------------------------------------------
    # 7. Create SQL views
    # ---------------------------------------------------------
    if args.create_views:
        logger.info("Creating analytical SQL views from: %s", config.views_sql_path)
        db.create_views()

    logger.info("CSV loader completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
