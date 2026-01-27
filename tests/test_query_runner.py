import pytest
from unittest.mock import Mock, call
from scripts.query_runner import QueryRunner


class TestQueryRunner:

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def mock_exporter(self):
        return Mock()

    @pytest.fixture
    def runner(self, mock_db, mock_exporter):
        return QueryRunner(mock_db, mock_exporter)

    def test_run_all_executes_each_query(self, runner, mock_db):
        query_class_1 = Mock()
        query_class_2 = Mock()
        query_instance_1 = Mock()
        query_instance_2 = Mock()

        query_class_1.return_value = query_instance_1
        query_class_2.return_value = query_instance_2

        query_instance_1.get_query_name.return_value = "query1"
        query_instance_2.get_query_name.return_value = "query2"
        query_instance_1.execute.return_value = [{"id": 1}]
        query_instance_2.execute.return_value = [{"id": 2}]

        runner.run_all([query_class_1, query_class_2], "output.json")

        query_instance_1.execute.assert_called_once()
        query_instance_2.execute.assert_called_once()

    def test_run_all_passes_db_to_queries(self, runner, mock_db):
        query_class = Mock()
        query_instance = Mock()
        query_class.return_value = query_instance
        query_instance.get_query_name.return_value = "test"
        query_instance.execute.return_value = []

        runner.run_all([query_class], "output.json")

        query_class.assert_called_once_with(mock_db)

    def test_run_all_collects_results_by_name(self, runner, mock_exporter):
        query_class = Mock()
        query_instance = Mock()
        query_class.return_value = query_instance
        query_instance.get_query_name.return_value = "my_query"
        query_instance.execute.return_value = [{"data": "value"}]

        runner.run_all([query_class], "output.json")

        export_call = mock_exporter.export.call_args
        results = export_call[0][0]
        assert "my_query" in results
        assert results["my_query"] == [{"data": "value"}]

    def test_run_all_exports_to_specified_path(self, runner, mock_exporter):
        query_class = Mock()
        query_instance = Mock()
        query_class.return_value = query_instance
        query_instance.get_query_name.return_value = "test"
        query_instance.execute.return_value = []

        runner.run_all([query_class], "custom/path/results.xml")

        mock_exporter.export.assert_called_once()
        export_call = mock_exporter.export.call_args
        assert export_call[0][1] == "custom/path/results.xml"

    def test_run_all_handles_empty_query_list(self, runner, mock_exporter):
        runner.run_all([], "output.json")

        mock_exporter.export.assert_called_once_with({}, "output.json")

    def test_run_all_aggregates_multiple_query_results(self, runner, mock_exporter):
        queries = []
        for i in range(3):
            query_class = Mock()
            query_instance = Mock()
            query_class.return_value = query_instance
            query_instance.get_query_name.return_value = f"query_{i}"
            query_instance.execute.return_value = [{"index": i}]
            queries.append(query_class)

        runner.run_all(queries, "output.json")

        export_call = mock_exporter.export.call_args
        results = export_call[0][0]
        assert len(results) == 3
        assert "query_0" in results
        assert "query_1" in results
        assert "query_2" in results