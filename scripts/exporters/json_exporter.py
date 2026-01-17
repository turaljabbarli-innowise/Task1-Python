"""JSON exporter for query results.

This module provides JSON format export functionality with proper
handling of PostgreSQL-specific data types like Decimal.
"""

import json
from decimal import Decimal
from typing import Dict, Any
from .base import BaseExporter


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal types.

    PostgreSQL returns numeric values as Python Decimal objects,
    which are not JSON serializable by default. This encoder
    converts them to floats.
    """

    def default(self, obj):
        """Convert non-serializable objects to serializable types.

        Args:
            obj: Object to serialize.

        Returns:
            Float representation if obj is Decimal,
            otherwise delegates to parent encoder.

        Raises:
            TypeError: If object is not serializable.
        """
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class JsonExporter(BaseExporter):
    """Exporter for JSON format output.

    Converts query results to formatted JSON with proper
    indentation and Decimal type handling.
    """

    def get_file_extension(self) -> str:
        """Return the JSON file extension.

        Returns:
            String 'json'.
        """
        return "json"

    def convert(self, data: Dict[str, Any]) -> str:
        """Convert data dictionary to formatted JSON string.

        Args:
            data: Dictionary containing query results.

        Returns:
            Pretty-printed JSON string with 2-space indentation.
        """
        return json.dumps(data, indent=2, cls=DecimalEncoder)