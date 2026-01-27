import pytest
import json
from unittest.mock import patch, mock_open
from decimal import Decimal
from scripts.exporters.json_exporter import JsonExporter, DecimalEncoder
from scripts.exporters.xml_exporter import XmlExporter


class TestDecimalEncoder:

    def test_encodes_decimal_as_float(self):
        data = {"value": Decimal("123.45")}

        result = json.dumps(data, cls=DecimalEncoder)

        assert "123.45" in result

    def test_handles_nested_decimals(self):
        data = {
            "results": [
                {"avg": Decimal("10.5")},
                {"avg": Decimal("20.75")}
            ]
        }

        result = json.dumps(data, cls=DecimalEncoder)
        parsed = json.loads(result)

        assert parsed["results"][0]["avg"] == 10.5
        assert parsed["results"][1]["avg"] == 20.75

    def test_passes_through_other_types(self):
        data = {"string": "hello", "int": 42, "float": 3.14}

        result = json.dumps(data, cls=DecimalEncoder)
        parsed = json.loads(result)

        assert parsed["string"] == "hello"
        assert parsed["int"] == 42
        assert parsed["float"] == 3.14


class TestJsonExporter:

    @pytest.fixture
    def exporter(self):
        return JsonExporter()

    def test_get_file_extension(self, exporter):
        assert exporter.get_file_extension() == "json"

    def test_convert_returns_valid_json(self, exporter):
        data = {"query1": [{"id": 1}, {"id": 2}]}

        result = exporter.convert(data)
        parsed = json.loads(result)

        assert parsed == data

    def test_convert_handles_decimals(self, exporter):
        data = {"avg_brightness": [{"value": Decimal("75.5")}]}

        result = exporter.convert(data)
        parsed = json.loads(result)

        assert parsed["avg_brightness"][0]["value"] == 75.5

    def test_convert_formats_with_indent(self, exporter):
        data = {"key": "value"}

        result = exporter.convert(data)

        assert "\n" in result
        assert "  " in result

    def test_export_writes_to_file(self, exporter):
        data = {"test": [1, 2, 3]}

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                exporter.export(data, "output/test.json")

        mock_file.assert_called_once_with("output/test.json", "w")
        handle = mock_file()
        written_content = handle.write.call_args[0][0]
        assert json.loads(written_content) == data

    def test_export_creates_parent_directory(self, exporter):
        data = {"test": "data"}

        with patch("builtins.open", mock_open()):
            with patch("pathlib.Path.mkdir") as mock_mkdir:
                exporter.export(data, "nested/path/file.json")

        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


class TestXmlExporter:

    @pytest.fixture
    def exporter(self):
        return XmlExporter()

    def test_get_file_extension(self, exporter):
        assert exporter.get_file_extension() == "xml"

    def test_convert_returns_xml_string(self, exporter):
        data = {"items": [{"name": "test"}]}

        result = exporter.convert(data)

        assert "<?xml" in result
        assert "<results>" in result
        assert "</results>" in result

    def test_convert_includes_data(self, exporter):
        data = {"locations": [{"name": "Kitchen"}]}

        result = exporter.convert(data)

        assert "Kitchen" in result
        assert "locations" in result

    def test_convert_handles_multiple_items(self, exporter):
        data = {
            "leaf_locations": [
                {"location_name": "Room1"},
                {"location_name": "Room2"}
            ]
        }

        result = exporter.convert(data)

        assert "Room1" in result
        assert "Room2" in result

    def test_export_writes_to_file(self, exporter):
        data = {"test": "data"}

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.mkdir"):
                exporter.export(data, "output/test.xml")

        mock_file.assert_called_once_with("output/test.xml", "w")

    def test_convert_result_is_string_not_bytes(self, exporter):
        data = {"test": "value"}

        result = exporter.convert(data)

        assert isinstance(result, str)
