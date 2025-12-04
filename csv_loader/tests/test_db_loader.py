from __future__ import annotations

from decimal import Decimal

from pyspark.sql import SparkSession, Row
from sqlalchemy.orm import Session

from app.loader import load_users, load_products, load_orders
from app.models import User, Product, Order


def _create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("csv_loader_db_tests")
        .master("local[1]")
        .getOrCreate()
    )


def test_load_users_products_orders(db_session: Session):
    spark = _create_spark()
    try:
        users_df = spark.createDataFrame(
            [
                Row(id=1, name="Alice", email="alice@example.com"),
                Row(id=2, name="Bob", email="bob@example.com"),
            ]
        )

        products_df = spark.createDataFrame(
            [
                Row(id=10, name="P1", price=10.0),
                Row(id=20, name="P2", price=15.5),
            ]
        )

        orders_df = spark.createDataFrame(
            [
                Row(id=100, user_id=1, product_id=10, quantity=2),
                Row(id=200, user_id=2, product_id=20, quantity=1),
            ]
        )

        load_users(users_df, db_session)
        load_products(products_df, db_session)
        load_orders(orders_df, db_session)

        users = db_session.query(User).order_by(User.id).all()
        products = db_session.query(Product).order_by(Product.id).all()
        orders = db_session.query(Order).order_by(Order.id).all()

        assert len(users) == 2
        assert users[0].email == "alice@example.com"

        assert len(products) == 2
        assert products[0].price == Decimal("10.00")

        assert len(orders) == 2
        assert orders[0].user_id == 1
        assert orders[0].product_id == 10
        assert orders[0].quantity == 2
    finally:
        spark.stop()
