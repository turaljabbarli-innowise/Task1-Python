import json
from typing import Dict, Any
from .base import BaseExporter


class JsonExporter(BaseExporter):
    def get_file_extension(self) -> str:
        return "json"

    def convert(self, data: Dict[str, Any]) -> str:
        return json.dumps(data, indent=2)

