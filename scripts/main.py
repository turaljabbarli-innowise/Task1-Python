import logging
from pathlib import Path
from config import Config
from database import DatabaseConnector
from importers import LocationImporter, DeviceImporter, EventImporter

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / "logs" / "etl_pipeline.log"
JSON_DIR = BASE_DIR / "jsons"

LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main() -> None:
    db_config = Config.get_db_params()

    if not all([db_config["dbname"], db_config["user"], db_config["password"]]):
        logging.critical("Missing database credentials.")
        return

    connector = DatabaseConnector(db_config)

    try:
        conn = connector.connect()

        LocationImporter(conn).import_data(str(JSON_DIR / "locations.json"))
        DeviceImporter(conn).import_data(str(JSON_DIR / "devices.json"))
        EventImporter(conn).import_data(str(JSON_DIR / "events.json"))

        logging.info("All ETL processes finished successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        connector.close()

if __name__ == "__main__":
    main()