"""File handling utilities for reading JSON data files.

This module provides static methods for reading and parsing JSON files
used in the IoT data pipeline.
"""

import json
import logging
from typing import List, Dict, Any


class FileHandler:
    """Utility class for file operations.

    Provides static methods for reading various file formats.
    Currently supports JSON file reading.
    """

    @staticmethod
    def read_json(file_path: str) -> List[Dict[str, Any]]:
        """Read and parse a JSON file containing a list of records.

        Args:
            file_path: Path to the JSON file to read.

        Returns:
            List of dictionaries parsed from the JSON file.
            Returns empty list if file cannot be read or parsed.
        """
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to read file {file_path}: {e}")
            return []