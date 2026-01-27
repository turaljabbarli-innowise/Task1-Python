"""Configuration module for application settings.

This module loads environment variables from .env file and provides
a Config class for accessing database connection parameters.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


class Config:
    """Application configuration loaded from environment variables.

    Provides class-level attributes for database connection parameters
    and a factory method for generating connection dictionaries.

    Attributes:
        DB_NAME: Database name from DB_NAME env variable.
        DB_USER: Database user from DB_USER env variable.
        DB_PASSWORD: Database password from DB_PASSWORD env variable.
        DB_HOST: Database host, defaults to 'localhost'.
        DB_PORT: Database port, defaults to '5432'.
    """

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")

    @classmethod
    def get_db_params(cls) -> dict:
        """Generate database connection parameters dictionary.

        Returns:
            Dictionary with keys: dbname, user, password, host, port.
            Compatible with psycopg2.connect() kwargs.
        """
        return {
            "dbname": cls.DB_NAME,
            "user": cls.DB_USER,
            "password": cls.DB_PASSWORD,
            "host": cls.DB_HOST,
            "port": cls.DB_PORT
        }
