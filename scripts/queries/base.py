"""Base query module defining the abstract interface for database queries.

This module provides the BaseQuery abstract class that all query classes
must inherit from, following the Template Method design pattern.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseQuery(ABC):
    """Abstract base class for database queries.

    Defines the interface for executing queries and transforming results
    into dictionaries. Subclasses must implement abstract methods to
    provide query-specific SQL and column definitions.

    This design follows the Single Responsibility Principle where each
    query class handles exactly one database query.

    Attributes:
        db: DatabaseManager instance for database operations.
    """

    def __init__(self, db_manager):
        """Initialize the query with a database manager.

        Args:
            db_manager: DatabaseManager instance for executing queries.
        """
        self.db = db_manager

    @abstractmethod
    def get_query_name(self) -> str:
        """Return a unique identifier for this query.

        Used as the key in result dictionaries when exporting.

        Returns:
            String identifier for the query.
        """
        pass

    @abstractmethod
    def get_sql(self) -> str:
        """Return the SQL query string to execute.

        Returns:
            SQL SELECT statement as a string.
        """
        pass

    @abstractmethod
    def get_columns(self) -> List[str]:
        """Return the list of column names in the result set.

        Must match the order of columns in the SELECT statement.

        Returns:
            List of column name strings.
        """
        pass

    def execute(self) -> List[Dict[str, Any]]:
        """Execute the query and return results as dictionaries.

        Returns:
            List of dictionaries where keys are column names
            and values are the corresponding row values.
        """
        rows = self.db.fetch_all(self.get_sql())
        return self._convert_to_dicts(rows)

    def _convert_to_dicts(self, rows: List[tuple]) -> List[Dict[str, Any]]:
        """Convert tuple rows to dictionaries using column names.

        Args:
            rows: List of tuples from database fetch.

        Returns:
            List of dictionaries with column names as keys.
        """
        columns = self.get_columns()
        return [dict(zip(columns, row)) for row in rows]