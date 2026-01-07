import json
from decimal import Decimal
from typing import Dict, Any
from .base import BaseExporter


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class JsonExporter(BaseExporter):
    def get_file_extension(self) -> str:
        return "json"

    def convert(self, data: Dict[str, Any]) -> str:
        return json.dumps(data, indent=2, cls=DecimalEncoder)