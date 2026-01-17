"""Base exporter module defining the abstract interface for data exporters.

This module provides the BaseExporter abstract class that all format-specific
exporters must inherit from, following the Template Method design pattern.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any


class BaseExporter(ABC):
    """Abstract base class for result exporters.

    Defines the interface for converting query results to different
    output formats and writing them to files. Subclasses must implement
    format-specific conversion logic.

    This design follows the Open/Closed Principle, allowing new export
    formats to be added without modifying existing code.
    """

    @abstractmethod
    def get_file_extension(self) -> str:
        """Return the file extension for this export format.

        Returns:
            String file extension without the leading dot.
        """
        pass

    @abstractmethod
    def convert(self, data: Dict[str, Any]) -> str:
        """Convert data dictionary to the target format string.

        Args:
            data: Dictionary containing query results to convert.

        Returns:
            String representation in the target format.
        """
        pass

    def export(self, data: Dict[str, Any], filepath: str) -> None:
        """Export data to a file in the target format.

        Creates parent directories if they don't exist, converts
        the data, and writes to the specified file.

        Args:
            data: Dictionary containing query results to export.
            filepath: Destination file path.
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = self.convert(data)
        with open(filepath, "w") as file:
            file.write(content)