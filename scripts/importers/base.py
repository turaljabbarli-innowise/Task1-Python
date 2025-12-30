from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseImporter(ABC):
    def __init__(self, db_manager):
        self.db = db_manager

    @abstractmethod
    def get_table_name(self) -> str:
        pass

    @abstractmethod
    def get_conflict_column(self) -> str:
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def process_entities(self, data: List[Dict[str, Any]]) -> None:
        for record in data:
            try:
                transformed_data = self.transform_data(record)
                self.db.insert(
                    table=self.get_table_name(),
                    data=transformed_data,
                    conflict_column=self.get_conflict_column()
                )
            except Exception as e:
                self.db.rollback()
                raise
        self.db.commit()