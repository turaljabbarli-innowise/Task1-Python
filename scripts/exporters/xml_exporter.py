from typing import Dict, Any
from dicttoxml import dicttoxml
from .base import BaseExporter


class XmlExporter(BaseExporter):
    def get_file_extension(self) -> str:
        return "xml"

    def convert(self, data: Dict[str, Any]) -> str:
        xml_bytes = dicttoxml(data, custom_root='results', attr_type=False)
        xml_string = xml_bytes.decode("utf-8")
        return xml_string