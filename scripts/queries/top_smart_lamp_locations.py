from typing import List
from .base import BaseQuery


class TopSmartLampLocationsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "top_smart_lamp_locations"

    def get_sql(self) -> str:
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
        return ["location_name", "device_count"]