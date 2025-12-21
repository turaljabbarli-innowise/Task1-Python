import json
import logging
import psycopg2
from typing import List, Dict, Any, Set
from psycopg2.extensions import connection

class LocationImporter:
    def __init__(self, db_conn: connection):
        self.conn = db_conn

    def import_data(self, file_path: str) -> None:
        try:
            with open(file_path, 'r') as file:
                data: List[Dict[str, Any]] = json.load(file)
            self._process_and_insert(data)
        except Exception as e:
            logging.error(f"Error during location import: {e}")

    def _process_and_insert(self, data: List[Dict[str, Any]]) -> None:
        cursor = self.conn.cursor()
        inserted_ids: Set[str] = set()
        to_insert = data

        while to_insert:
            deferred: List[Dict[str, Any]] = []
            progress_made = False
            for item in to_insert:
                loc_id = str(item.get('location_id'))
                p_id = item.get('parent_location_id')
                p_id = str(p_id) if p_id is not None else None

                if p_id == loc_id:
                    p_id = None

                if p_id is None or p_id in inserted_ids:
                    try:
                        cursor.execute("""
                            INSERT INTO locations (location_id, parent_location_id, location_name)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (location_id) DO NOTHING;
                        """, (loc_id, p_id, item.get('location_name')))
                        inserted_ids.add(loc_id)
                        progress_made = True
                    except psycopg2.Error:
                        self.conn.rollback()
                else:
                    deferred.append(item)

            if not progress_made and deferred:
                logging.warning(f"Skipping {len(deferred)} orphan locations.")
                break
            to_insert = deferred

        self.conn.commit()
        cursor.close()