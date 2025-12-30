import json
import logging
from typing import List, Dict, Any

class FileHandler:
    @staticmethod
    def read_json(file_path: str) -> List[Dict[str, Any]]:
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to read file {file_path}: {e}")
            return []