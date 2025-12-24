import json
from typing import Dict, Any
from .base import BaseImporter


class EventImporter(BaseImporter):
    def get_table_name(self) -> str:
        return "events"

    def get_conflict_column(self) -> str:
        return "event_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        e_id = raw_data.get('event_id')
        details = raw_data.get('details', {}).copy()

        dev_id = details.pop('device_id', None)
        ts = details.pop('timestamp', None)

        return {
            "event_id": e_id,
            "device_id": dev_id,
            "timestamp": ts,
            "details": json.dumps(details)
        }