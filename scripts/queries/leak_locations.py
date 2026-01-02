from typing import List
from .base import BaseQuery


class LeakLocationsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "leak_locations"

    def get_sql(self) -> str:
        return """
            SELECT DISTINCT locations.location_name
            FROM locations
            JOIN devices ON devices.location_id = locations.location_id
            JOIN events ON events.device_id = devices.device_id
            WHERE events.details->>'leak_detected' = 'true'
        """

    def get_columns(self) -> List[str]:
        return ["location_name"]