import json
from unittest.mock import mock_open, patch
from scripts.file_handler import FileHandler


class TestFileHandler:

    def test_read_json_returns_parsed_data(self):
        fake_data = [{"id": 1, "name": "Test"}]
        fake_json = json.dumps(fake_data)

        with patch("builtins.open", mock_open(read_data=fake_json)):
            result = FileHandler.read_json("fake/path.json")

        assert result == fake_data

    def test_read_json_returns_empty_list_on_file_not_found(self):
        with patch("builtins.open", side_effect=FileNotFoundError("No such file")):
            result = FileHandler.read_json("nonexistent.json")

        assert result == []

    def test_read_json_returns_empty_list_on_invalid_json(self):
        invalid_json = "{ this is not valid json }"

        with patch("builtins.open", mock_open(read_data=invalid_json)):
            result = FileHandler.read_json("bad.json")

        assert result == []

    def test_read_json_handles_empty_file(self):
        with patch("builtins.open", mock_open(read_data="")):
            result = FileHandler.read_json("empty.json")

        assert result == []

    def test_read_json_handles_nested_structures(self):
        nested_data = [
            {
                "location_id": "loc1",
                "children": [{"id": "child1"}, {"id": "child2"}]
            }
        ]
        fake_json = json.dumps(nested_data)

        with patch("builtins.open", mock_open(read_data=fake_json)):
            result = FileHandler.read_json("nested.json")

        assert result == nested_data
        assert len(result[0]["children"]) == 2
