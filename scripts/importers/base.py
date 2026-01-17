"""Base importer module defining the abstract interface for data importers.

This module provides the BaseImporter abstract class that all entity-specific
importers must inherit from, following the Template Method design pattern.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseImporter(ABC):
    """Abstract base class for entity importers.

    Defines the interface and common functionality for importing data
    from parsed JSON into database tables. Subclasses must implement
    the abstract methods to specify table-specific behavior.

    This class follows the Template Method pattern where process_entities
    defines the algorithm structure, and subclasses provide specific implementations.

    Attributes:
        db: DatabaseManager instance for database operations.
    """

    def __init__(self, db_manager):
        """Initialize the importer with a database manager.

        Args:
            db_manager: DatabaseManager instance for executing database operations.
        """
        self.db = db_manager

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the name of the target database table.

        Returns:
            String name of the table this importer writes to.
        """
        pass

    @abstractmethod
    def get_conflict_column(self) -> str:
        """Return the column name used for conflict resolution.

        Returns:
            String name of the primary key or unique column.
        """
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw JSON data into database-ready format.

        Args:
            raw_data: Dictionary containing raw data from JSON file.

        Returns:
            Dictionary with keys matching database column names
            and values ready for insertion.
        """
        pass

    def process_entities(self, data: List[Dict[str, Any]]) -> None:
        """Process and insert a list of entities into the database.

        Iterates through each record, transforms it, and inserts it
        into the target table. Commits all changes at the end.

        Args:
            data: List of dictionaries containing raw entity data.

        Raises:
            Exception: If any insert operation fails. Transaction is
                rolled back before re-raising.
        """
        for record in data:
            try:
                transformed_data = self.transform_data(record)
                self.db.insert(
                    table=self.get_table_name(),
                    data=transformed_data,
                    conflict_column=self.get_conflict_column()
                )
            except Exception as e:
                self.db.rollback()
                raise
        self.db.commit()