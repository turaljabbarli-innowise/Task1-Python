import logging
from typing import Dict, Any, List, Set
from .base import BaseImporter


class LocationImporter(BaseImporter):
    def get_table_name(self) -> str:
        return "locations"

    def get_conflict_column(self) -> str:
        return "location_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "location_id": raw_data.get("location_id"),
            "parent_location_id": raw_data.get("parent_location_id"),
            "location_name": raw_data.get("location_name")
        }

    def process_entities(self, data: List[Dict[str, Any]]) -> None:
        inserted_ids: Set[str] = set()
        to_insert = data

        while to_insert:
            deferred = []
            progress = False

            for item in to_insert:
                loc_id = str(item.get('location_id'))
                p_id = item.get('parent_location_id')
                p_id = str(p_id) if p_id is not None else None

                if p_id == loc_id:
                    p_id = None

                if p_id is None or p_id in inserted_ids:
                    try:
                        transformed_data = self.transform_data(item)
                        self.db.insert(
                            table=self.get_table_name(),
                            data=transformed_data,
                            conflict_column=self.get_conflict_column()
                        )
                        inserted_ids.add(loc_id)
                        progress = True
                    except Exception:
                        self.db.rollback()
                        raise
                else:
                    deferred.append(item)

            if not progress and deferred:
                logging.warning(f"Could not insert {len(deferred)} locations due to missing parents")
                break

            to_insert = deferred

        self.db.commit()