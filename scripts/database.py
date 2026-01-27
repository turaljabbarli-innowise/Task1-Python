"""Database management module for PostgreSQL connections and operations.

This module provides a DatabaseManager class that handles all database
interactions including connections, CRUD operations, and transaction management.
"""

import psycopg2
import logging
from typing import Dict, Any, Optional, List
from psycopg2.extensions import connection


class DatabaseManager:
    """Manages PostgreSQL database connections and operations.

    This class provides methods for connecting to a PostgreSQL database,
    executing queries, and managing transactions. It follows the context
    manager pattern for safe resource handling.

    Attributes:
        config: Database connection parameters.
        conn: Active database connection or None if not connected.
    """

    def __init__(self, config: Dict[str, str]):
        """Initialize DatabaseManager with connection configuration.

        Args:
            config: Dictionary containing database connection parameters
                including 'dbname', 'user', 'password', 'host', and 'port'.
        """
        self.config = config
        self.conn: Optional[connection] = None

    def connect(self) -> None:
        """Establish a connection to the PostgreSQL database.

        Raises:
            psycopg2.Error: If connection cannot be established.
        """
        try:
            self.conn = psycopg2.connect(**self.config)
            logging.info("Database connection established successfully.")
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def insert(self, table: str, data: Dict[str, Any], conflict_column: Optional[str] = None) -> None:
        """Insert a single record into the specified table.

        Constructs and executes an INSERT query with optional conflict handling
        using PostgreSQL's ON CONFLICT clause.

        Args:
            table: Name of the target table.
            data: Dictionary mapping column names to values.
            conflict_column: Column name for ON CONFLICT DO NOTHING clause.
                If None, duplicate key violations will raise an exception.

        Raises:
            RuntimeError: If no database connection exists.
            psycopg2.Error: If the insert operation fails.
        """
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
        """Execute a SQL query without returning results.

        Suitable for DDL statements or DML operations where results
        are not needed.

        Args:
            query: SQL query string to execute.
            params: Optional tuple of parameters for query placeholders.

        Raises:
            RuntimeError: If no database connection exists.
            psycopg2.Error: If query execution fails.
        """
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
        """Execute a query and fetch a single result row.

        Args:
            query: SQL SELECT query string.
            params: Optional tuple of parameters for query placeholders.

        Returns:
            A tuple containing the row data, or None if no rows match.

        Raises:
            RuntimeError: If no database connection exists.
            psycopg2.Error: If query execution fails.
        """
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
        """Execute a query and fetch all result rows.

        Args:
            query: SQL SELECT query string.
            params: Optional tuple of parameters for query placeholders.

        Returns:
            List of tuples, where each tuple represents a row.
            Returns empty list if no rows match.

        Raises:
            RuntimeError: If no database connection exists.
            psycopg2.Error: If query execution fails.
        """
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
        """Commit the current transaction.

        Raises:
            psycopg2.Error: If commit operation fails.
        """
        if self.conn:
            try:
                self.conn.commit()
                logging.debug("Transaction committed successfully.")
            except psycopg2.Error as e:
                logging.error(f"Failed to commit transaction: {e}")
                raise

    def rollback(self) -> None:
        """Rollback the current transaction.

        Reverts all changes made since the last commit.

        Raises:
            psycopg2.Error: If rollback operation fails.
        """
        if self.conn:
            try:
                self.conn.rollback()
                logging.warning("Transaction rolled back.")
            except psycopg2.Error as e:
                logging.error(f"Failed to rollback transaction: {e}")
                raise

    def close(self) -> None:
        """Close the database connection.

        Releases the connection back to the system. After calling this method,
        connect() must be called again before any database operations.

        Raises:
            psycopg2.Error: If closing the connection fails.
        """
        if self.conn:
            try:
                self.conn.close()
                logging.info("Database connection closed.")
                self.conn = None
            except psycopg2.Error as e:
                logging.error(f"Failed to close connection: {e}")
                raise
