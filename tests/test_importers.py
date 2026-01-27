import pytest
from unittest.mock import Mock
from scripts.importers.devices import DeviceImporter
from scripts.importers.events import EventImporter
from scripts.importers.locations import LocationImporter


class TestDeviceImporter:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def importer(self, mock_db):
        return DeviceImporter(mock_db)

    def test_get_table_name(self, importer):
        assert importer.get_table_name() == "devices"

    def test_get_conflict_column(self, importer):
        assert importer.get_conflict_column() == "device_id"

    def test_transform_data_extracts_fields(self, importer):
        raw = {
            "device_id": "d1",
            "device_type": "Smart Lamp",
            "device_name": "Living Room Lamp",
            "location_id": "loc1"
        }

        result = importer.transform_data(raw)

        assert result["device_id"] == "d1"
        assert result["device_type"] == "Smart Lamp"
        assert result["device_name"] == "Living Room Lamp"
        assert result["location_id"] == "loc1"

    def test_transform_data_handles_missing_fields(self, importer):
        raw = {"device_id": "d1"}

        result = importer.transform_data(raw)

        assert result["device_id"] == "d1"
        assert result["device_type"] is None
        assert result["device_name"] is None
        assert result["location_id"] is None

    def test_process_entities_inserts_all_records(self, mock_db, importer):
        data = [
            {"device_id": "d1", "device_type": "Lamp", "device_name": "L1", "location_id": "loc1"},
            {"device_id": "d2", "device_type": "Sensor", "device_name": "S1", "location_id": "loc2"}
        ]

        importer.process_entities(data)

        assert mock_db.insert.call_count == 2
        mock_db.commit.assert_called_once()

    def test_process_entities_calls_insert_with_correct_params(self, mock_db, importer):
        data = [{"device_id": "d1", "device_type": "Lamp", "device_name": "L1", "location_id": "loc1"}]

        importer.process_entities(data)

        mock_db.insert.assert_called_once_with(
            table="devices",
            data={
                "device_id": "d1",
                "device_type": "Lamp",
                "device_name": "L1",
                "location_id": "loc1"
            },
            conflict_column="device_id"
        )


class TestEventImporter:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def importer(self, mock_db):
        return EventImporter(mock_db)

    def test_get_table_name(self, importer):
        assert importer.get_table_name() == "events"

    def test_get_conflict_column(self, importer):
        assert importer.get_conflict_column() == "event_id"

    def test_transform_data_extracts_device_id_and_timestamp(self, importer):
        raw = {
            "event_id": "e1",
            "details": {
                "device_id": "d1",
                "timestamp": "2024-01-01T10:00:00",
                "brightness": 80,
                "new_status": "on"
            }
        }

        result = importer.transform_data(raw)

        assert result["event_id"] == "e1"
        assert result["device_id"] == "d1"
        assert result["timestamp"] == "2024-01-01T10:00:00"
        assert "device_id" not in result["details"]
        assert "timestamp" not in result["details"]

    def test_transform_data_details_is_json_string(self, importer):
        import json
        raw = {
            "event_id": "e1",
            "details": {
                "device_id": "d1",
                "timestamp": "2024-01-01T10:00:00",
                "brightness": 80
            }
        }

        result = importer.transform_data(raw)

        details_dict = json.loads(result["details"])
        assert details_dict["brightness"] == 80

    def test_transform_data_preserves_remaining_fields(self, importer):
        import json
        raw = {
            "event_id": "e1",
            "details": {
                "device_id": "d1",
                "timestamp": "2024-01-01",
                "brightness": 50,
                "new_status": "on",
                "color": "warm"
            }
        }

        result = importer.transform_data(raw)
        details = json.loads(result["details"])

        assert details["brightness"] == 50
        assert details["new_status"] == "on"
        assert details["color"] == "warm"


class TestLocationImporter:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def importer(self, mock_db):
        return LocationImporter(mock_db)

    def test_get_table_name(self, importer):
        assert importer.get_table_name() == "locations"

    def test_get_conflict_column(self, importer):
        assert importer.get_conflict_column() == "location_id"

    def test_transform_data(self, importer):
        raw = {
            "location_id": "loc1",
            "parent_location_id": "parent1",
            "location_name": "Living Room"
        }

        result = importer.transform_data(raw)

        assert result["location_id"] == "loc1"
        assert result["parent_location_id"] == "parent1"
        assert result["location_name"] == "Living Room"

    def test_process_entities_handles_hierarchy(self, mock_db, importer):
        data = [
            {"location_id": "child", "parent_location_id": "parent", "location_name": "Child"},
            {"location_id": "parent", "parent_location_id": None, "location_name": "Parent"}
        ]

        importer.process_entities(data)

        calls = mock_db.insert.call_args_list
        first_inserted = calls[0][1]["data"]["location_id"]
        second_inserted = calls[1][1]["data"]["location_id"]

        assert first_inserted == "parent"
        assert second_inserted == "child"

    def test_process_entities_handles_self_reference(self, mock_db, importer):
        data = [
            {"location_id": "loc1", "parent_location_id": "loc1", "location_name": "Self Ref"}
        ]

        importer.process_entities(data)

        mock_db.insert.assert_called_once()

    def test_process_entities_commits_at_end(self, mock_db, importer):
        data = [{"location_id": "loc1", "parent_location_id": None, "location_name": "Test"}]

        importer.process_entities(data)

        mock_db.commit.assert_called_once()

    def test_process_entities_rollback_on_error(self, mock_db, importer):
        mock_db.insert.side_effect = Exception("DB Error")
        data = [{"location_id": "loc1", "parent_location_id": None, "location_name": "Test"}]

        with pytest.raises(Exception):
            importer.process_entities(data)

        mock_db.rollback.assert_called()
