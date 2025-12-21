import json
import logging
import psycopg2
from typing import List, Dict, Any
from psycopg2.extensions import connection

class EventImporter:
    def __init__(self, db_conn: connection):
        self.conn = db_conn

    def import_data(self, file_path: str) -> None:
        try:
            with open(file_path, 'r') as file:
                data: List[Dict[str, Any]] = json.load(file)
            self._process_events(data)
        except Exception as e:
            logging.error(f"Error during event import: {e}")

    def _process_events(self, data: List[Dict[str, Any]]) -> None:
        cursor = self.conn.cursor()
        for record in data:
            event_id = record.get('event_id')
            details = record.get('details', {})
            device_id = details.pop('device_id', None)
            timestamp = details.pop('timestamp', None)

            try:
                cursor.execute("""
                    INSERT INTO events (event_id, device_id, timestamp, details)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """, (event_id, device_id, timestamp, json.dumps(details)))
            except psycopg2.Error as e:
                logging.warning(f"Skipping event {event_id}: {e.pgerror}")
                self.conn.rollback()
        self.conn.commit()
        cursor.close()