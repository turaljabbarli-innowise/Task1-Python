"""Query for finding locations without any sublocations.

This module identifies leaf nodes in the location hierarchy tree.
"""

from typing import List
from .base import BaseQuery


class LeafLocationsQuery(BaseQuery):
    """Query to find all locations that have no child locations.

    Uses a LEFT JOIN with self-reference to identify locations
    that are not the parent of any other location.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'leaf_locations'.
        """
        return "leaf_locations"

    def get_sql(self) -> str:
        """Return SQL to find locations without sublocations.

        Returns:
            SQL query using LEFT JOIN to find leaf locations.
        """
        return """
            SELECT l.location_name
            FROM locations l
            LEFT JOIN locations sub ON l.location_id = sub.parent_location_id
            WHERE sub.location_id IS NULL
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name'.
        """
        return ["location_name"]
