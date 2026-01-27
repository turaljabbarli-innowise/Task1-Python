import pytest
from unittest.mock import Mock
from decimal import Decimal
from scripts.queries.leaf_locations import LeafLocationsQuery
from scripts.queries.lowest_sublocations import LowestSublocationsQuery
from scripts.queries.smart_lamp_events import SmartLampEventsQuery
from scripts.queries.avg_brightness import AvgBrightnessQuery
from scripts.queries.leak_locations import LeakLocationsQuery
from scripts.queries.devices_no_events import DevicesNoEventsQuery
from scripts.queries.top_smart_lamp_locations import TopSmartLampLocationsQuery


class TestLeafLocationsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return LeafLocationsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "leaf_locations"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name"]

    def test_get_sql_uses_left_join(self, query):
        sql = query.get_sql()
        assert "LEFT JOIN" in sql
        assert "WHERE sub.location_id IS NULL" in sql

    def test_execute_returns_list_of_dicts(self, mock_db, query):
        mock_db.fetch_all.return_value = [("Kitchen",), ("Bedroom",)]

        result = query.execute()

        assert result == [
            {"location_name": "Kitchen"},
            {"location_name": "Bedroom"}
        ]


class TestLowestSublocationsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return LowestSublocationsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "lowest_sublocations"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name", "lowest_sublocation"]

    def test_get_sql_uses_recursive_cte(self, query):
        sql = query.get_sql()
        assert "WITH RECURSIVE" in sql
        assert "hierarchy" in sql

    def test_execute_maps_columns_correctly(self, mock_db, query):
        mock_db.fetch_all.return_value = [
            ("Building A", "Room 101"),
            ("Building B", "Room 202")
        ]

        result = query.execute()

        assert result[0]["location_name"] == "Building A"
        assert result[0]["lowest_sublocation"] == "Room 101"


class TestSmartLampEventsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return SmartLampEventsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "smart_lamp_events"

    def test_get_columns(self, query):
        assert query.get_columns() == ["event_id"]

    def test_get_sql_filters_correctly(self, query):
        sql = query.get_sql()
        assert "Smart Lamp" in sql
        assert "new_status" in sql
        assert "brightness" in sql
        assert "> 80" in sql

    def test_execute_returns_event_ids(self, mock_db, query):
        mock_db.fetch_all.return_value = [("e1",), ("e2",), ("e3",)]

        result = query.execute()

        assert len(result) == 3
        assert result[0] == {"event_id": "e1"}


class TestAvgBrightnessQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return AvgBrightnessQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "average_brightness"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name", "average_brightness"]

    def test_get_sql_uses_avg_and_group_by(self, query):
        sql = query.get_sql()
        assert "AVG" in sql
        assert "GROUP BY" in sql
        assert "location_name" in sql

    def test_execute_handles_decimal_values(self, mock_db, query):
        mock_db.fetch_all.return_value = [
            ("Kitchen", Decimal("75.5")),
            ("Bedroom", Decimal("60.0"))
        ]

        result = query.execute()

        assert result[0]["location_name"] == "Kitchen"
        assert result[0]["average_brightness"] == Decimal("75.5")


class TestLeakLocationsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return LeakLocationsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "leak_locations"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name"]

    def test_get_sql_checks_leak_detected(self, query):
        sql = query.get_sql()
        assert "leak_detected" in sql
        assert "true" in sql.lower()

    def test_get_sql_uses_distinct(self, query):
        sql = query.get_sql()
        assert "DISTINCT" in sql


class TestDevicesNoEventsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return DevicesNoEventsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "devices_no_events"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name", "device_name"]

    def test_get_sql_uses_left_join_and_null_check(self, query):
        sql = query.get_sql()
        assert "LEFT JOIN events" in sql
        assert "IS NULL" in sql

    def test_execute_returns_location_and_device(self, mock_db, query):
        mock_db.fetch_all.return_value = [
            ("Kitchen", "Unused Sensor"),
            ("Garage", "Old Lamp")
        ]

        result = query.execute()

        assert result[0]["location_name"] == "Kitchen"
        assert result[0]["device_name"] == "Unused Sensor"


class TestTopSmartLampLocationsQuery:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def query(self, mock_db):
        return TopSmartLampLocationsQuery(mock_db)

    def test_get_query_name(self, query):
        assert query.get_query_name() == "top_smart_lamp_locations"

    def test_get_columns(self, query):
        assert query.get_columns() == ["location_name", "device_count"]

    def test_get_sql_uses_count_and_limit(self, query):
        sql = query.get_sql()
        assert "COUNT" in sql
        assert "LIMIT 3" in sql
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_execute_returns_top_three(self, mock_db, query):
        mock_db.fetch_all.return_value = [
            ("Living Room", 5),
            ("Bedroom", 3),
            ("Kitchen", 2)
        ]

        result = query.execute()

        assert len(result) == 3
        assert result[0]["device_count"] == 5
        assert result[2]["device_count"] == 2
