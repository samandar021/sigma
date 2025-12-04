from __future__ import annotations

import logging
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from .config import AppConfig

logger = logging.getLogger(__name__)


def create_spark_session(config: AppConfig) -> SparkSession:
    builder = SparkSession.builder.appName(config.spark.app_name)
    if config.spark.master:
        builder = builder.master(config.spark.master)
    spark = builder.getOrCreate()
    logger.info("SparkSession created with app name '%s'.", config.spark.app_name)
    return spark


def _users_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("email", StringType(), nullable=False),
        ]
    )


def _products_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
        ]
    )


def _orders_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("product_id", IntegerType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
        ]
    )


def read_csvs(
    spark: SparkSession,
    users_path: str,
    products_path: str,
    orders_path: str,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    logger.info("Reading users from %s", users_path)
    users_df = (
        spark.read.csv(users_path, header=True, schema=_users_schema())
        .dropna(subset=["id", "name", "email"])
    )

    logger.info("Reading products from %s", products_path)
    products_df = (
        spark.read.csv(products_path, header=True, schema=_products_schema())
        .dropna(subset=["id", "name", "price"])
    )

    logger.info("Reading orders from %s", orders_path)
    orders_df = (
        spark.read.csv(orders_path, header=True, schema=_orders_schema())
        .dropna(subset=["id", "user_id", "product_id", "quantity"])
    )

    logger.info(
        "Raw counts: users=%d, products=%d, orders=%d",
        users_df.count(),
        products_df.count(),
        orders_df.count(),
    )

    return users_df, products_df, orders_df


def validate_users(users_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Validate users: require a syntactically valid email.
    """
    email_regex = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    users_valid = users_df.filter(
        F.col("email").rlike(email_regex)
    )

    users_invalid = users_df.exceptAll(users_valid)

    logger.info(
        "Users validation: valid=%d, invalid=%d",
        users_valid.count(),
        users_invalid.count(),
    )

    return users_valid, users_invalid


def validate_products(products_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Validate products: price > 0.
    """
    products_valid = products_df.filter(F.col("price") > 0)
    products_invalid = products_df.filter(F.col("price") <= 0)

    logger.info(
        "Products validation: valid=%d, invalid=%d",
        products_valid.count(),
        products_invalid.count(),
    )

    return products_valid, products_invalid


def validate_orders(
    orders_df: DataFrame,
    users_valid: DataFrame,
    products_valid: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate orders:
    - quantity >= 1
    - user_id exists in users
    - product_id exists in products
    """
    orders_non_negative = orders_df.filter(F.col("quantity") >= 1)

    users_ids = users_valid.select("id").withColumnRenamed("id", "user_id_ref")
    orders_with_users = orders_non_negative.join(
        users_ids,
        orders_non_negative.user_id == F.col("user_id_ref"),
        how="left",
    )

    products_ids = products_valid.select("id").withColumnRenamed("id", "product_id_ref")
    orders_with_all_refs = orders_with_users.join(
        products_ids,
        orders_with_users.product_id == F.col("product_id_ref"),
        how="left",
    )

    orders_valid = orders_with_all_refs.filter(
        F.col("user_id_ref").isNotNull() & F.col("product_id_ref").isNotNull()
    ).select("id", "user_id", "product_id", "quantity")

    orders_invalid = orders_non_negative.exceptAll(orders_valid)

    logger.info(
        "Orders validation: valid=%d, invalid=%d",
        orders_valid.count(),
        orders_invalid.count(),
    )

    return orders_valid, orders_invalid
