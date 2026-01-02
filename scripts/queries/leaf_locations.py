from typing import List
from .base import BaseQuery


class LeafLocationsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "leaf_locations"

    def get_sql(self) -> str:
        return """
            SELECT l.location_name
            FROM locations l
            LEFT JOIN locations sub ON l.location_id = sub.parent_location_id
            WHERE sub.location_id IS NULL
        """

    def get_columns(self) -> List[str]:
        return ["location_name"]