"""XML exporter for query results.

This module provides XML format export functionality using the
dicttoxml library for automatic conversion.
"""

from typing import Dict, Any
from dicttoxml import dicttoxml
from .base import BaseExporter


class XmlExporter(BaseExporter):
    """Exporter for XML format output.

    Converts query results to XML using dicttoxml library,
    with a custom root element and without type attributes.
    """

    def get_file_extension(self) -> str:
        """Return the XML file extension.

        Returns:
            String 'xml'.
        """
        return "xml"

    def convert(self, data: Dict[str, Any]) -> str:
        """Convert data dictionary to XML string.

        Uses dicttoxml to generate XML with 'results' as the
        root element and without type attributes for cleaner output.

        Args:
            data: Dictionary containing query results.

        Returns:
            UTF-8 decoded XML string.
        """
        xml_bytes = dicttoxml(data, custom_root='results', attr_type=False)
        xml_string = xml_bytes.decode("utf-8")
        return xml_string
