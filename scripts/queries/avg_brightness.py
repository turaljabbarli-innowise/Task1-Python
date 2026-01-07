from typing import List
from .base import BaseQuery


class AvgBrightnessQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "average_brightness"

    def get_sql(self) -> str:
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
        return ['location_name', 'average_brightness']