from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .config import AppConfig
from .models import Base

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._engine: Engine = create_engine(
            config.db.sqlalchemy_url,
            echo=False,
            future=True,
        )
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )
        logger.info("Database engine created for %s", config.db.sqlalchemy_url)

    @property
    def engine(self) -> Engine:
        return self._engine

    @contextmanager
    def session(self) -> Iterator[Session]:
        """
        Provide a transactional scope around a series of operations:

        with db.session() as session:
            load_users(..., session)
        """
        session: Session = self._SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Error in DB session, rolled back.")
            raise
        finally:
            session.close()

    def init_db(self) -> None:
        """
        Create all tables defined in models.Base metadata.
        """
        logger.info("Creating database schema (if not exists)...")
        Base.metadata.create_all(self._engine)
        logger.info("Database schema initialized.")

    def create_views(self) -> None:
        """
        Execute SQL from the views.sql file to create analytical views.
        """
        views_path = Path(self._config.views_sql_path)
        if not views_path.exists():
            logger.warning("Views SQL file not found at %s; skipping.", views_path)
            return

        sql_text = views_path.read_text(encoding="utf-8")
        statements = [
            s.strip()
            for s in sql_text.split(";")
            if s.strip()
        ]

        if not statements:
            logger.warning("Views SQL file %s is empty; nothing to run.", views_path)
            return

        logger.info("Creating views from %s ...", views_path)
        with self._engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Views created successfully.")
