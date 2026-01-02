from typing import List
from .base import BaseQuery


class SmartLampEventsQuery(BaseQuery):
    def get_query_name(self) -> str:
        return "smart_lamp_events"

    def get_sql(self) -> str:
        return """
            SELECT event_id
            FROM events
            JOIN devices ON devices.device_id = events.device_id
            WHERE devices.device_type = 'Smart Lamp'
            AND events.details->>'new_status' = 'on'
            AND (events.details->>'brightness')::int > 80
        """

    def get_columns(self) -> List[str]:
        return ['event_id']