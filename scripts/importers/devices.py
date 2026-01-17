"""Device importer for IoT device data.

This module handles importing device records and linking them
to their corresponding locations.
"""

from typing import Dict, Any
from .base import BaseImporter


class DeviceImporter(BaseImporter):
    """Importer for IoT device entities.

    Handles importing devices with their type, name, and location
    references. Devices must be imported after locations due to
    foreign key constraints.
    """

    def get_table_name(self) -> str:
        """Return the devices table name.

        Returns:
            String 'devices'.
        """
        return "devices"

    def get_conflict_column(self) -> str:
        """Return the primary key column for conflict resolution.

        Returns:
            String 'device_id'.
        """
        return "device_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw device data for database insertion.

        Args:
            raw_data: Dictionary containing device_id, device_type,
                device_name, and location_id fields.

        Returns:
            Dictionary formatted for the devices table.
        """
        return {
            "device_id": raw_data.get("device_id"),
            "device_type": raw_data.get("device_type"),
            "device_name": raw_data.get("device_name"),
            "location_id": raw_data.get("location_id")
        }