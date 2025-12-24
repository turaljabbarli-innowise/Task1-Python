from typing import Dict, Any
from .base import BaseImporter


class DeviceImporter(BaseImporter):
    def get_table_name(self) -> str:
        return "devices"

    def get_conflict_column(self) -> str:
        return "device_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "device_id": raw_data.get("device_id"),
            "device_type": raw_data.get("device_type"),
            "device_name": raw_data.get("device_name"),
            "location_id": raw_data.get("location_id")
        }