from abc import ABC, abstractmethod
from psycopg2.extensions import connection

class BaseImporter(ABC):
    def __init__(self, db_conn: connection):
        self.conn = db_conn

    @abstractmethod
    def import_data(self, file_path: str) -> None:
        pass