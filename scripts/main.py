import logging
from database import DatabaseConnector
from importers.locations import LocationImporter
from importers.devices import DeviceImporter
from importers.events import EventImporter

logging.basicConfig(
    filename='../logs/etl_pipeline.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def main() -> None:
    db_config = {
        "dbname": "iot_db",
        "user": "myuser",
        "password": "mypassword",
        "host": "localhost",
        "port": "5432"
    }

    connector = DatabaseConnector(db_config)

    try:
        conn = connector.connect()

        LocationImporter(conn).import_data("../jsons/locations.json")
        DeviceImporter(conn).import_data("../jsons/devices.json")
        EventImporter(conn).import_data("../jsons/events.json")

        logging.info("All ETL processes finished successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        connector.close()


if __name__ == "__main__":
    main()