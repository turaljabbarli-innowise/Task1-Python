from typing import List
from .base import BaseQuery


class DevicesNoEventsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "devices_no_events"

    def get_sql(self) -> str:
        return """
            SELECT locations.location_name, devices.device_name
            FROM locations
            JOIN devices ON devices.location_id = locations.location_id
            LEFT JOIN events ON events.device_id = devices.device_id
            WHERE events.event_id IS NULL
        """

    def get_columns(self) -> List[str]:
        return ["location_name", "device_name"]