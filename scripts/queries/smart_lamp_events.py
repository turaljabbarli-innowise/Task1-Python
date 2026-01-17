"""Query for finding high-brightness Smart Lamp activation events.

This module queries events where Smart Lamps were turned on
with brightness exceeding a threshold.
"""

from typing import List
from .base import BaseQuery


class SmartLampEventsQuery(BaseQuery):
    """Query to find Smart Lamp events with high brightness settings.

    Filters events for Smart Lamp devices where the lamp was turned on
    with brightness greater than 80.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'smart_lamp_events'.
        """
        return "smart_lamp_events"

    def get_sql(self) -> str:
        """Return SQL to find high-brightness Smart Lamp on events.

        Uses JSONB operators to extract and filter on nested fields
        within the events details column.

        Returns:
            SQL query filtering on device type, status, and brightness.
        """
        return """
            SELECT event_id
            FROM events
            JOIN devices ON devices.device_id = events.device_id
            WHERE devices.device_type = 'Smart Lamp'
            AND events.details->>'new_status' = 'on'
            AND (events.details->>'brightness')::int > 80
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'event_id'.
        """
        return ['event_id']