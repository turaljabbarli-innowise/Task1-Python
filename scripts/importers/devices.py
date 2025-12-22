import logging
from .base import BaseImporter

class DeviceImporter(BaseImporter):
    def process_entities(self, data: list) -> None:
        for d in data:
            try:
                self.db.insert_device(
                    d.get('device_id'), d.get('device_type'),
                    d.get('device_name'), d.get('location_id')
                )
            except Exception:
                self.db.rollback()
        self.db.commit()