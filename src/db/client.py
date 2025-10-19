from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.models import Base

import os
from contextlib import contextmanager

from src.utils.logger import logger

log = logger.bind(step="DB")



class DatabaseClient:
    """
    Client for interacting with the SQLite DB
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(self.db_url, connect_args={"check_same_thread": False}, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)


    def init_db(self):
        """
        Initializes the database if it doesn't exist.
        """
        if not os.path.exists(self.db_path):
            log.info(f"Initializing DB at {self.db_path}")
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            Base.metadata.create_all(bind=self.engine)
            log.success(f"DB initialized at {self.db_path}.")
        else:
            log.debug(f"DB already exists at {self.db_path}.")

    @contextmanager
    def get_session(self):
        """
        Generator that provides a SQLAlchemy session.
        """
        try:
            session = self.SessionLocal()
            yield session
        finally:
            session.close()

    def get_engine_conn(self):
        """
        Returns an engine connection.
        """
        return self.engine.connect()
        








if __name__ == "__main__":
    db_client = DatabaseClient(db_path="data/app.db")
    db_client.init_db()
    
