"""Query for finding locations with the most Smart Lamp devices.

This module ranks locations by their Smart Lamp device count
and returns the top three.
"""

from typing import List
from .base import BaseQuery


class TopSmartLampLocationsQuery(BaseQuery):
    """Query to find top 3 locations by Smart Lamp count.

    Aggregates device counts per location, filtering for Smart Lamp
    device type, and returns the top 3 locations by count.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'top_smart_lamp_locations'.
        """
        return "top_smart_lamp_locations"

    def get_sql(self) -> str:
        """Return SQL to find top 3 locations by Smart Lamp count.

        Groups devices by location, counts Smart Lamps, orders
        descending, and limits to 3 results.

        Returns:
            SQL query with GROUP BY, ORDER BY, and LIMIT clauses.
        """
        return """
            SELECT l.location_name, COUNT(d.device_id) AS device_count
            FROM locations l
            JOIN devices d ON l.location_id = d.location_id
            WHERE d.device_type = 'Smart Lamp'
            GROUP BY l.location_name
            ORDER BY device_count DESC
            LIMIT 3
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name' and 'device_count'.
        """
        return ["location_name", "device_count"]
