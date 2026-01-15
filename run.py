import argparse
import logging
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

from pathlib import Path

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


def parse_args():
    parser = argparse.ArgumentParser(description="IoT Data Pipeline - Load data and run queries")

    parser.add_argument("--locations", type=str, required=True, help="Path to locations file")
    parser.add_argument("--devices", type=str, required=True, help="Path to devices file")
    parser.add_argument("--events", type=str, required=True, help="Path to events file")
    parser.add_argument("--format", type=str, required=False,default="xml",choices=["json", "xml"], help="Preferred export type of the output (json(default xml))")


    return parser.parse_args()


def main():
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

        runner.run_all(all_queries, f"output/results.{args.format}")
        print(f"Check output/results.{args.format}")

    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()