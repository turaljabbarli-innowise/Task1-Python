from typing import List
from .base import BaseQuery


class LowestSublocationsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "lowest_sublocations"

    def get_sql(self) -> str:
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
        return ["location_name", "lowest_sublocation"]