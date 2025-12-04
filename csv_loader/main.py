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
    log_dir = os.getenv("LOG_DIR", "logs")
    setup_logging(log_dir=log_dir)

    args = parse_args(argv)

    config = AppConfig.from_env()
    config.users_csv = args.users
    config.products_csv = args.products
    config.orders_csv = args.orders

    logger.info("Starting CSV loader application...")
    logger.info("Users CSV: %s", config.users_csv)
    logger.info("Products CSV: %s", config.products_csv)
    logger.info("Orders CSV: %s", config.orders_csv)

    db = Database(config=config)

    if args.init_db:
        logger.info("Initializing database schema...")
        db.init_db()

    logger.info("Creating Spark session...")
    spark = create_spark_session(config)

    logger.info("Reading CSVs with Spark...")
    users_df, products_df, orders_df = read_csvs(
        spark,
        users_path=config.users_csv,
        products_path=config.products_csv,
        orders_path=config.orders_csv,
    )

    logger.info("Validating users...")
    users_valid, users_invalid = validate_users(users_df)

    logger.info("Validating products...")
    products_valid, products_invalid = validate_products(products_df)

    logger.info("Validating orders...")
    orders_valid, orders_invalid = validate_orders(
        orders_df,
        users_valid=users_valid,
        products_valid=products_valid,
    )

    logger.info("Loading validated data into PostgreSQL...")
    with db.session() as session:
        load_users(users_valid, session)
        load_products(products_valid, session)
        load_orders(orders_valid, session)

    # 5) Create views if requested
    if args.create_views:
        logger.info("Creating views from %s", config.views_sql_path)
        db.create_views()

    logger.info("CSV loader completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
