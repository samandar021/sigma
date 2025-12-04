# app/config.py
from dataclasses import dataclass
import os


@dataclass
class DBConfig:
    host: str
    port: int
    name: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        """
        Build SQLAlchemy connection URL from pieces.

        Example:
        postgresql+psycopg2://csv_user:csv_password@db:5432/csv_db
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class SparkConfig:
    app_name: str
    master: str | None = None


@dataclass
class AppConfig:
    db: DBConfig
    users_csv: str
    products_csv: str
    orders_csv: str
    views_sql_path: str
    spark: SparkConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        db = DBConfig(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            name=os.getenv("DB_NAME", "csv_db"),
            user=os.getenv("DB_USER", "csv_user"),
            password=os.getenv("DB_PASSWORD", "csv_password"),
        )

        spark = SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", "csv_loader"),
            master=os.getenv("SPARK_MASTER"),
        )

        return cls(
            db=db,
            users_csv=os.getenv("USERS_CSV_PATH", "/app/data/users_big.csv"),
            products_csv=os.getenv("PRODUCTS_CSV_PATH", "/app/data/products_big.csv"),
            orders_csv=os.getenv("ORDERS_CSV_PATH", "/app/data/orders_big.csv"),
            views_sql_path=os.getenv("VIEWS_SQL_PATH", "/app/sql/views.sql"),
            spark=spark,
        )
