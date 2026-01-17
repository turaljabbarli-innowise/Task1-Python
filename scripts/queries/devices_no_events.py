"""Query for finding devices that have generated no events.

This module identifies devices in the system that have never
recorded any event data.
"""

from typing import List
from .base import BaseQuery


class DevicesNoEventsQuery(BaseQuery):
    """Query to find devices without any associated events.

    Uses a LEFT JOIN between devices and events to identify
    devices that have no matching event records.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'devices_no_events'.
        """
        return "devices_no_events"

    def get_sql(self) -> str:
        """Return SQL to find devices with no events.

        Joins locations to devices for location context, then
        LEFT JOINs to events and filters for NULL event_id.

        Returns:
            SQL query using LEFT JOIN to find eventless devices.
        """
        return """
            SELECT locations.location_name, devices.device_name
            FROM locations
            JOIN devices ON devices.location_id = locations.location_id
            LEFT JOIN events ON events.device_id = devices.device_id
            WHERE events.event_id IS NULL
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name' and 'device_name'.
        """
        return ["location_name", "device_name"]