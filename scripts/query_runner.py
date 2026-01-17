"""Query runner module for executing and exporting multiple queries.

This module provides the QueryRunner class that orchestrates
the execution of multiple queries and exports results.
"""

from typing import List, Type


class QueryRunner:
    """Orchestrates execution of multiple queries and result export.

    Acts as a facade that coordinates between query classes and
    exporters, handling the workflow of running queries and
    saving results to a file.

    Attributes:
        db: DatabaseManager instance for query execution.
        exporter: BaseExporter instance for result output.
    """

    def __init__(self, db_manager, exporter):
        """Initialize QueryRunner with database and exporter.

        Args:
            db_manager: DatabaseManager instance for database operations.
            exporter: BaseExporter subclass instance for output formatting.
        """
        self.db = db_manager
        self.exporter = exporter

    def run_all(self, queries: List[Type], output_path: str) -> None:
        """Execute all queries and export results to a single file.

        Instantiates each query class, executes it, collects results
        into a dictionary keyed by query name, and exports to the
        specified file path.

        Args:
            queries: List of BaseQuery subclass types to execute.
            output_path: Destination file path for exported results.
        """
        results = {}

        for QueryClass in queries:
            query = QueryClass(self.db)
            name = query.get_query_name()
            data = query.execute()
            results[name] = data

        self.exporter.export(results, output_path)