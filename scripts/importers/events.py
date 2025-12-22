import logging
from .base import BaseImporter

class EventImporter(BaseImporter):
    def process_entities(self, data: list) -> None:
        for record in data:
            e_id = record.get('event_id')
            details = record.get('details', {}).copy()
            dev_id = details.pop('device_id', None)
            ts = details.pop('timestamp', None)
            try:
                self.db.insert_event(e_id, dev_id, ts, details)
            except Exception:
                self.db.rollback()
        self.db.commit()