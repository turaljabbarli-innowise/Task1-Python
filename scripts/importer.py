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
                loc_id = item.get('location_id')
                p_id = item.get('parent_location_id')

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
                logging.warning(f"Skipping {len(deferred)} orphan locations due to missing parents.")
                break
            to_insert = deferred

        self.conn.commit()
        cursor.close()


class DeviceImporter:
    def __init__(self, db_conn: connection):
        self.conn = db_conn

    def import_data(self, file_path: str) -> None:
        try:
            with open(file_path, 'r') as file:
                data: List[Dict[str, Any]] = json.load(file)
            self._insert_devices(data)
        except Exception as e:
            logging.error(f"Error during device import: {e}")

    def _insert_devices(self, data: List[Dict[str, Any]]) -> None:
        cursor = self.conn.cursor()
        for device in data:
            try:
                cursor.execute("""
                    INSERT INTO devices (device_id, device_type, device_name, location_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (device_id) DO NOTHING;
                """, (device.get('device_id'), device.get('device_type'), device.get('device_name'),
                      device.get('location_id')))
            except psycopg2.Error as e:
                logging.warning(f"Skipping device {device.get('device_id')}: {e.pgerror}")
                self.conn.rollback()
        self.conn.commit()
        cursor.close()


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

            # Use pop() to extract these and leave everything else for JSONB
            device_id = details.pop('device_id', None)
            timestamp = details.pop('timestamp', None)

            try:
                cursor.execute("""
                    INSERT INTO events (event_id, device_id, timestamp, details)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """, (event_id, device_id, timestamp, json.dumps(details)))
            except psycopg2.Error as e:
                logging.warning(f"Skipping event {event_id} for device {device_id}: {e.pgerror}")
                self.conn.rollback()
        self.conn.commit()
        cursor.close()