from __future__ import annotations

import logging
from decimal import Decimal
from typing import Iterable

from pyspark.sql import DataFrame
from sqlalchemy.orm import Session

from .models import User, Product, Order

logger = logging.getLogger(__name__)


def _df_to_dict_iterator(df: DataFrame) -> Iterable[dict]:
    """
    Stream rows from a Spark DataFrame as Python dicts.
    Using toLocalIterator() to avoid loading everything into memory at once.
    """
    for row in df.toLocalIterator():
        yield row.asDict(recursive=True)


def load_users(users_df: DataFrame, session: Session) -> None:
    """
    Load valid users into PostgreSQL via SQLAlchemy.
    """
    logger.info("Loading users into PostgreSQL...")
    objs: list[User] = []

    for row in _df_to_dict_iterator(users_df):
        try:
            objs.append(
                User(
                    id=int(row["id"]),
                    name=str(row["name"]),
                    email=str(row["email"]),
                )
            )
        except Exception:
            logger.exception("Skipping invalid user row during load: %s", row)

    if not objs:
        logger.info("No users to insert.")
        return

    try:
        session.bulk_save_objects(objs)
        session.flush()
        logger.info("Inserted %d users.", len(objs))
    except Exception:
        logger.exception("Error inserting users batch.")
        raise


def load_products(products_df: DataFrame, session: Session) -> None:
    """
    Load valid products into PostgreSQL via SQLAlchemy.
    """
    logger.info("Loading products into PostgreSQL...")
    objs: list[Product] = []

    for row in _df_to_dict_iterator(products_df):
        try:
            raw_price = row["price"]
            price = Decimal(str(raw_price))
            objs.append(
                Product(
                    id=int(row["id"]),
                    name=str(row["name"]),
                    price=price,
                )
            )
        except Exception:
            logger.exception("Skipping invalid product row during load: %s", row)

    if not objs:
        logger.info("No products to insert.")
        return

    try:
        session.bulk_save_objects(objs)
        session.flush()
        logger.info("Inserted %d products.", len(objs))
    except Exception:
        logger.exception("Error inserting products batch.")
        raise


def load_orders(orders_df: DataFrame, session: Session) -> None:
    """
    Load valid orders into PostgreSQL via SQLAlchemy.
    Assumes corresponding users/products already exist (FK validation done in Spark).
    """
    logger.info("Loading orders into PostgreSQL...")
    objs: list[Order] = []

    for row in _df_to_dict_iterator(orders_df):
        try:
            objs.append(
                Order(
                    id=int(row["id"]),
                    user_id=int(row["user_id"]),
                    product_id=int(row["product_id"]),
                    quantity=int(row["quantity"]),
                )
            )
        except Exception:
            logger.exception("Skipping invalid order row during load: %s", row)

    if not objs:
        logger.info("No orders to insert.")
        return

    try:
        session.bulk_save_objects(objs)
        session.flush()
        logger.info("Inserted %d orders.", len(objs))
    except Exception:
        logger.exception("Error inserting orders batch.")
        raise
