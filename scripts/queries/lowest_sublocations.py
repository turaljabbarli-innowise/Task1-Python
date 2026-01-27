"""Query for finding the deepest sublocation for each root location.

This module uses recursive CTEs to traverse the location hierarchy
and find the lowest level sublocation for each top-level location.
"""

from typing import List
from .base import BaseQuery


class LowestSublocationsQuery(BaseQuery):
    """Query to find the deepest sublocation for each location hierarchy.

    Uses a recursive Common Table Expression (CTE) to traverse the
    location tree and identify the maximum depth sublocation for
    each root location.
    """

    def get_query_name(self) -> str:
        """Return the query identifier.

        Returns:
            String 'lowest_sublocations'.
        """
        return "lowest_sublocations"

    def get_sql(self) -> str:
        """Return SQL using recursive CTE to find deepest sublocations.

        The recursive CTE builds the complete hierarchy with depth tracking,
        then selects only the maximum depth nodes for each root.

        Returns:
            SQL query with recursive CTE for hierarchy traversal.
        """
        return """
            WITH RECURSIVE hierarchy AS (
                SELECT
                    location_id,
                    location_name,
                    location_id AS root_id,
                    location_name AS root_name,
                    0 AS depth
                FROM locations

                UNION ALL

                SELECT
                    child.location_id,
                    child.location_name,
                    parent.root_id,
                    parent.root_name,
                    parent.depth + 1
                FROM locations child
                JOIN hierarchy parent ON child.parent_location_id = parent.location_id
            )
            SELECT root_name AS location_name, location_name AS lowest_sublocation
            FROM hierarchy h1
            WHERE depth = (SELECT MAX(depth) FROM hierarchy h2 WHERE h2.root_id = h1.root_id)
              AND depth > 0
            ORDER BY root_name
        """

    def get_columns(self) -> List[str]:
        """Return the result column names.

        Returns:
            List containing 'location_name' and 'lowest_sublocation'.
        """
        return ["location_name", "lowest_sublocation"]
