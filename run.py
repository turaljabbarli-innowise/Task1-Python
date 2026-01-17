"""IoT Data Pipeline CLI application.

This module provides the command-line interface for the IoT data pipeline.
It loads data from JSON files into PostgreSQL and executes analytical queries,
exporting results to JSON or XML format.

Usage:
    python run.py --locations <path> --devices <path> --events <path> [--format json|xml]

Example:
    python run.py --locations jsons/locations.json --devices jsons/devices.json \
                  --events jsons/events.json --format json
"""

import argparse
import logging
from pathlib import Path

from config import Config
from scripts.database import DatabaseManager
from scripts.file_handler import FileHandler
from scripts.importers import LocationImporter, DeviceImporter, EventImporter
from scripts.exporters import JsonExporter, XmlExporter
from scripts.query_runner import QueryRunner
from scripts.queries import (
    LeafLocationsQuery,
    LowestSublocationsQuery,
    SmartLampEventsQuery,
    AvgBrightnessQuery,
    LeakLocationsQuery,
    DevicesNoEventsQuery,
    TopSmartLampLocationsQuery
)

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "logs" / "etl_pipeline.log"
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logging.getLogger('dicttoxml').setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace object containing parsed arguments:
        - locations: Path to locations JSON file.
        - devices: Path to devices JSON file.
        - events: Path to events JSON file.
        - format: Output format ('json' or 'xml'), defaults to 'xml'.
    """
    parser = argparse.ArgumentParser(
        description="IoT Data Pipeline - Load data and run queries"
    )

    parser.add_argument(
        "--locations",
        type=str,
        required=True,
        help="Path to locations JSON file"
    )
    parser.add_argument(
        "--devices",
        type=str,
        required=True,
        help="Path to devices JSON file"
    )
    parser.add_argument(
        "--events",
        type=str,
        required=True,
        help="Path to events JSON file"
    )
    parser.add_argument(
        "--format",
        type=str,
        required=False,
        default="xml",
        choices=["json", "xml"],
        help="Output format for query results (default: xml)"
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point for the IoT data pipeline.

    Performs the following steps:
    1. Parses command-line arguments
    2. Connects to PostgreSQL database
    3. Loads data from JSON files into database tables
    4. Executes all analytical queries
    5. Exports results to the specified format
    """
    args = parse_args()

    db_config = Config.get_db_params()
    db = DatabaseManager(db_config)

    try:
        db.connect()

        locations_data = FileHandler.read_json(args.locations)
        LocationImporter(db).process_entities(locations_data)

        devices_data = FileHandler.read_json(args.devices)
        DeviceImporter(db).process_entities(devices_data)

        events_data = FileHandler.read_json(args.events)
        EventImporter(db).process_entities(events_data)

        logging.info("All ETL processes finished successfully.")

        if args.format == "json":
            exporter = JsonExporter()
        else:
            exporter = XmlExporter()

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

        output_file = f"output/results.{args.format}"
        runner.run_all(all_queries, output_file)
        print(f"Results exported to {output_file}")

    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()