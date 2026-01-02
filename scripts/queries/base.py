from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseQuery(ABC):
    def __init__(self, db_manager):
        self.db = db_manager

    @abstractmethod
    def get_query_name(self) -> str:
        pass

    @abstractmethod
    def get_sql(self) -> str:
        pass

    @abstractmethod
    def get_columns(self) -> List[str]:
        pass

    def execute(self) -> List[Dict[str, Any]]:
        rows = self.db.fetch_all(self.get_sql())
        return self._convert_to_dicts(rows)

    def _convert_to_dicts(self, rows: List[tuple]) -> List[Dict[str, Any]]:
        columns = self.get_columns()
        return [dict(zip(columns, row)) for row in rows]
