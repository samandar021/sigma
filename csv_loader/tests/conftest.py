from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.models import Base


@dataclass
class TestDBConfig:
    sqlalchemy_url: str


@dataclass
class TestAppConfig:
    db: TestDBConfig
    spark = type("SparkCfg", (), {"app_name": "test_app", "master": "local[1]"})()
    log_dir: str = "logs"
    views_sql_path: str = "sql/views.sql"


@pytest.fixture(scope="session")
def sqlite_engine():
    fd, path = tempfile.mkstemp(prefix="csv_loader_test_", suffix=".sqlite")
    os.close(fd)

    url = f"sqlite:///{path}"
    engine = create_engine(url, echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture()
def db_session(sqlite_engine) -> Session:
    SessionLocal = sessionmaker(bind=sqlite_engine, autoflush=False, autocommit=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    finally:
        session.close()


@pytest.fixture(scope="session")
def test_config(sqlite_engine):
    return TestAppConfig(
        db=TestDBConfig(sqlalchemy_url=str(sqlite_engine.url))
    )
