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


def main():
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