import json
import logging
import psycopg2
from typing import List, Dict, Any
from psycopg2.extensions import connection

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