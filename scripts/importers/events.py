"""Event importer for IoT event data.

This module handles importing events with nested JSON details,
extracting key fields while preserving the remaining data as JSONB.
"""

import json
from typing import Dict, Any
from .base import BaseImporter


class EventImporter(BaseImporter):
    """Importer for IoT event entities.

    Handles the transformation of nested event data where device_id
    and timestamp are extracted from the details object and stored
    as separate columns, while remaining details are stored as JSONB.
    """

    def get_table_name(self) -> str:
        """Return the events table name.

        Returns:
            String 'events'.
        """
        return "events"

    def get_conflict_column(self) -> str:
        """Return the primary key column for conflict resolution.

        Returns:
            String 'event_id'.
        """
        return "event_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw event data for database insertion.

        Extracts device_id and timestamp from the nested details object
        into separate columns. The remaining details are serialized
        to JSON string for JSONB storage.

        Args:
            raw_data: Dictionary with event_id and nested details object
                containing device_id, timestamp, and event-specific fields.

        Returns:
            Dictionary with event_id, device_id, timestamp as separate
            fields, and remaining details as JSON string.
        """
        e_id = raw_data.get('event_id')
        details = raw_data.get('details', {}).copy()

        dev_id = details.pop('device_id', None)
        ts = details.pop('timestamp', None)

        return {
            "event_id": e_id,
            "device_id": dev_id,
            "timestamp": ts,
            "details": json.dumps(details)
        }
