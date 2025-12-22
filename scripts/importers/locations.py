import logging
from .base import BaseImporter

class LocationImporter(BaseImporter):
    def process_entities(self, data: list) -> None:
        inserted_ids = set()
        to_insert = data
        while to_insert:
            deferred = []
            progress = False
            for item in to_insert:
                loc_id = str(item.get('location_id'))
                p_id = item.get('parent_location_id')
                p_id = str(p_id) if p_id is not None else None
                if p_id == loc_id: p_id = None
                if p_id is None or p_id in inserted_ids:
                    try:
                        self.db.insert_location(loc_id, p_id, item.get('location_name'))
                        inserted_ids.add(loc_id)
                        progress = True
                    except Exception:
                        self.db.rollback()
                else:
                    deferred.append(item)
            if not progress and deferred:
                break
            to_insert = deferred
        self.db.commit()