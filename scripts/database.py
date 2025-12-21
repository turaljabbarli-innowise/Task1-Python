import psycopg2
import logging
from typing import Dict, Optional
from psycopg2.extensions import connection

class DatabaseConnector:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn: Optional[connection] = None

    def connect(self) -> connection:
        try:
            self.conn = psycopg2.connect(**self.config)
            logging.info("Database connection established successfully.")
            return self.conn
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")