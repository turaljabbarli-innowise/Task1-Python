"""Query for calculating average Smart Lamp brightness by location.

This module aggregates brightness values for Smart Lamp on events,
grouped by location.
"""

from typing import List
from .base import BaseQuery


class AvgBrightnessQuery(BaseQuery):
    """Query to calculate average brightness per location.

    Computes the arithmetic mean of brightness values for all
    Smart Lamp activation events, grouped by location name.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'average_brightness'.
        """
        return "average_brightness"

    def get_sql(self) -> str:
        """Return SQL to calculate average brightness by location.

        Joins locations, devices, and events tables, filtering for
        Smart Lamp on events and computing AVG on brightness.

        Returns:
            SQL query with aggregation grouped by location.
        """
        return """
            SELECT locations.location_name, AVG((events.details->>'brightness')::int) AS average_brightness
                FROM locations
                JOIN devices ON devices.location_id = locations.location_id
                JOIN events ON events.device_id = devices.device_id
                WHERE devices.device_type = 'Smart Lamp'
                AND events.details->>'new_status' = 'on'
                GROUP BY locations.location_name
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name' and 'average_brightness'.
        """
        return ['location_name', 'average_brightness']
