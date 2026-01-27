import pytest
from unittest.mock import Mock, patch
from scripts.database import DatabaseManager


class TestDatabaseManagerConnection:

    def test_connect_establishes_connection(self):
        config = {"dbname": "test", "user": "user", "password": "pass"}
        db = DatabaseManager(config)

        with patch("scripts.database.psycopg2.connect") as mock_connect:
            mock_connect.return_value = Mock()
            db.connect()

        mock_connect.assert_called_once_with(**config)
        assert db.conn is not None

    def test_connect_raises_on_failure(self):
        import psycopg2
        config = {"dbname": "test", "user": "user", "password": "pass"}
        db = DatabaseManager(config)

        with patch("scripts.database.psycopg2.connect") as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection failed")

            with pytest.raises(psycopg2.Error):
                db.connect()

    def test_close_closes_connection(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        mock_conn = Mock()
        db.conn = mock_conn

        db.close()

        mock_conn.close.assert_called_once()
        assert db.conn is None

    def test_close_does_nothing_when_not_connected(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        db.conn = None

        db.close()


class TestDatabaseManagerInsert:

    @pytest.fixture
    def connected_db(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        db.conn = Mock()
        return db

    def test_insert_executes_correct_query(self, connected_db):
        mock_cursor = Mock()
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        connected_db.insert("devices", {"device_id": "d1", "device_name": "Lamp"})

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        values = call_args[0][1]

        assert "INSERT INTO devices" in query
        assert "device_id" in query
        assert "device_name" in query
        assert "d1" in values
        assert "Lamp" in values

    def test_insert_with_conflict_column(self, connected_db):
        mock_cursor = Mock()
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        connected_db.insert(
            "devices",
            {"device_id": "d1"},
            conflict_column="device_id"
        )

        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]

        assert "ON CONFLICT (device_id) DO NOTHING" in query

    def test_insert_raises_when_not_connected(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        db.conn = None

        with pytest.raises(RuntimeError, match="Database connection not established"):
            db.insert("devices", {"device_id": "d1"})

    def test_insert_skips_empty_data(self, connected_db):
        connected_db.insert("devices", {})

        connected_db.conn.cursor.assert_not_called()


class TestDatabaseManagerFetch:

    @pytest.fixture
    def connected_db(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        db.conn = Mock()
        return db

    def test_fetch_one_returns_single_row(self, connected_db):
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("result_value",)
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        result = connected_db.fetch_one("SELECT * FROM test")

        assert result == ("result_value",)

    def test_fetch_one_returns_none_when_no_results(self, connected_db):
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        result = connected_db.fetch_one("SELECT * FROM test WHERE 1=0")

        assert result is None

    def test_fetch_all_returns_all_rows(self, connected_db):
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("row1",), ("row2",), ("row3",)]
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        result = connected_db.fetch_all("SELECT * FROM test")

        assert len(result) == 3
        assert result[0] == ("row1",)

    def test_fetch_all_with_params(self, connected_db):
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("filtered",)]
        connected_db.conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        connected_db.conn.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %s", (1,))

    def test_fetch_raises_when_not_connected(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        db.conn = None

        with pytest.raises(RuntimeError):
            db.fetch_all("SELECT 1")


class TestDatabaseManagerTransaction:

    def test_commit_commits_transaction(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        mock_conn = Mock()
        db.conn = mock_conn

        db.commit()

        mock_conn.commit.assert_called_once()

    def test_rollback_rolls_back_transaction(self):
        config = {"dbname": "test"}
        db = DatabaseManager(config)
        mock_conn = Mock()
        db.conn = mock_conn

        db.rollback()

        mock_conn.rollback.assert_called_once()
