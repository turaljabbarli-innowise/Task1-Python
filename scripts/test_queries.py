"""Standalone script for testing query execution.

This module provides a quick way to test all queries without
going through the full CLI. Exports results to JSON format.

Note:
    Requires database to be running and populated with data.
"""

from config import Config
from database import DatabaseManager
from exporters import JsonExporter
from query_runner import QueryRunner
from queries import (
    LeafLocationsQuery,
    LowestSublocationsQuery,
    SmartLampEventsQuery,
    AvgBrightnessQuery,
    LeakLocationsQuery,
    DevicesNoEventsQuery,
    TopSmartLampLocationsQuery
)


def main() -> None:
    """Execute all queries and export results to JSON.

    Connects to the database, runs all query classes, and
    saves results to output/results.json.
    """
    db = DatabaseManager(Config.get_db_params())
    db.connect()

    exporter = JsonExporter()
    runner = QueryRunner(db, exporter)

    all_queries = [
        LeafLocationsQuery,
        LowestSublocationsQuery,
        SmartLampEventsQuery,
        AvgBrightnessQuery,
        LeakLocationsQuery,
        DevicesNoEventsQuery,
        TopSmartLampLocationsQuery
    ]

    runner.run_all(all_queries, "output/results.json")
    print("Check output/results.json")

    db.close()


if __name__ == "__main__":
    main()