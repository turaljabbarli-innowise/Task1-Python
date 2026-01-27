"""Query for finding locations with leak detection events.

This module identifies locations that have devices reporting
water leak detections.
"""

from typing import List
from .base import BaseQuery


class LeakLocationsQuery(BaseQuery):
    """Query to find locations where leaks have been detected.

    Joins locations through devices to events, filtering for
    events where leak_detected is true in the JSONB details.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'leak_locations'.
        """
        return "leak_locations"

    def get_sql(self) -> str:
        """Return SQL to find locations with leak events.

        Uses DISTINCT to ensure each location appears only once
        even if multiple leak events occurred.

        Returns:
            SQL query filtering on leak_detected JSONB field.
        """
        return """
            SELECT DISTINCT locations.location_name
            FROM locations
            JOIN devices ON devices.location_id = locations.location_id
            JOIN events ON events.device_id = devices.device_id
            WHERE events.details->>'leak_detected' = 'true'
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name'.
        """
        return ["location_name"]
