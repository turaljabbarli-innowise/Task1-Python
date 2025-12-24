import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / "logs" / "etl_pipeline.log"
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

import sys

sys.path.insert(0, str(BASE_DIR / "scripts"))

from config import Config
from database import DatabaseManager
from file_handler import FileHandler
from importers import LocationImporter, DeviceImporter, EventImporter


def main() -> None:
    db_config = Config.get_db_params()
    db = DatabaseManager(db_config)

    try:
        db.connect()

        json_dir = BASE_DIR / "jsons"

        locations_data = FileHandler.read_json(str(json_dir / "locations.json"))
        LocationImporter(db).process_entities(locations_data)

        devices_data = FileHandler.read_json(str(json_dir / "devices.json"))
        DeviceImporter(db).process_entities(devices_data)

        events_data = FileHandler.read_json(str(json_dir / "events.json"))
        EventImporter(db).process_entities(events_data)

        logging.info("All ETL processes finished successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()