import psycopg2
import logging
import json
from typing import Dict, Any, Optional
from psycopg2.extensions import connection

class DatabaseManager:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn: Optional[connection] = None

    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(**self.config)
            logging.info("Database connection established successfully.")
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def insert_location(self, loc_id: str, p_id: Optional[str], name: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO locations (location_id, parent_location_id, location_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (location_id) DO NOTHING;
            """, (loc_id, p_id, name))

    def insert_device(self, d_id: str, d_type: str, d_name: str, l_id: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO devices (device_id, device_type, device_name, location_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id) DO NOTHING;
            """, (d_id, d_type, d_name, l_id))

    def insert_event(self, e_id: str, dev_id: str, ts: str, details: Dict[str, Any]) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO events (event_id, device_id, timestamp, details)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """, (e_id, dev_id, ts, json.dumps(details)))

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn:
            self.conn.rollback()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")