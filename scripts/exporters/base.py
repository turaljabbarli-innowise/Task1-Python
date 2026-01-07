from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any


class BaseExporter(ABC):
    @abstractmethod
    def get_file_extension(self) -> str:
        pass

    @abstractmethod
    def convert(self, data: Dict[str, Any]) -> str:
        pass

    def export(self, data: Dict[str, Any], filepath: str) -> None:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = self.convert(data)
        with open(filepath, "w") as file:
            file.write(content)