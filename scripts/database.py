import psycopg2
import logging
from typing import Dict, Any, Optional, List
from psycopg2.extensions import connection


class DatabaseManager:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn: Optional[connection] = None

    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(**self.config)
            logging.info("Database connection established successfully.")
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def insert(self, table: str, data: Dict[str, Any], conflict_column: Optional[str] = None) -> None:

        if not self.conn:
            raise RuntimeError("Database connection not established. Call connect() first.")

        if not data:
            logging.warning(f"Attempted to insert empty data into {table}")
            return

        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_string = ', '.join(columns)

        query = f"""
            INSERT INTO {table} ({columns_string})
            VALUES ({placeholders})
        """

        if conflict_column:
            query += f" ON CONFLICT ({conflict_column}) DO NOTHING"

        query += ";"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, values)
                logging.debug(f"Inserted data into {table}: {data}")
        except psycopg2.Error as e:
            logging.error(f"Failed to insert into {table}: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None) -> None:

        if not self.conn:
            raise RuntimeError("Database connection not established. Call connect() first.")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                logging.debug(f"Executed query: {query}")
        except psycopg2.Error as e:
            logging.error(f"Failed to execute query: {e}")
            raise

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[tuple]:

        if not self.conn:
            raise RuntimeError("Database connection not established. Call connect() first.")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                logging.debug(f"Fetched one result for query: {query}")
                return result
        except psycopg2.Error as e:
            logging.error(f"Failed to fetch data: {e}")
            raise

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[tuple]:

        if not self.conn:
            raise RuntimeError("Database connection not established. Call connect() first.")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                logging.debug(f"Fetched {len(results)} results for query: {query}")
                return results
        except psycopg2.Error as e:
            logging.error(f"Failed to fetch data: {e}")
            raise

    def commit(self) -> None:

        if self.conn:
            try:
                self.conn.commit()
                logging.debug("Transaction committed successfully.")
            except psycopg2.Error as e:
                logging.error(f"Failed to commit transaction: {e}")
                raise

    def rollback(self) -> None:

        if self.conn:
            try:
                self.conn.rollback()
                logging.warning("Transaction rolled back.")
            except psycopg2.Error as e:
                logging.error(f"Failed to rollback transaction: {e}")
                raise

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
                logging.info("Database connection closed.")
                self.conn = None
            except psycopg2.Error as e:
                logging.error(f"Failed to close connection: {e}")
                raise