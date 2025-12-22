import logging
from pathlib import Path

# 1. SETUP LOGGING FIRST
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

# 2. THEN IMPORT OTHER MODULES
from config import Config
from database import DatabaseManager
from importers import LocationImporter, DeviceImporter, EventImporter


def main() -> None:
    db_config = Config.get_db_params()
    db = DatabaseManager(db_config)

    try:
        db.connect()

        json_dir = BASE_DIR / "jsons"
        LocationImporter(db).import_data(str(json_dir / "locations.json"))
        DeviceImporter(db).import_data(str(json_dir / "devices.json"))
        EventImporter(db).import_data(str(json_dir / "events.json"))

        logging.info("All ETL processes finished successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()