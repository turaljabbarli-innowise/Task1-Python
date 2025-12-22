from abc import ABC, abstractmethod
from database import DatabaseManager
from file_handler import FileHandler

class BaseImporter(ABC):
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def import_data(self, file_path: str) -> None:
        data = FileHandler.read_json(file_path)
        if data:
            self.process_entities(data)

    @abstractmethod
    def process_entities(self, data: list) -> None:
        pass