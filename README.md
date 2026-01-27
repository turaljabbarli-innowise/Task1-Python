# IoT Data Pipeline

A Python-based ETL pipeline that loads IoT device data from JSON files into PostgreSQL and executes analytical queries with JSON/XML export.

## Features

- Load hierarchical location data with parent-child relationships
- Import IoT devices and their events
- Store event details as JSONB for flexible querying
- Execute 7 pre-built analytical queries
- Export results to JSON or XML format

## Project Structure

```
.
├── config.py                 # Environment configuration
├── run.py                    # Main CLI entry point
├── docker-compose.yml        # PostgreSQL container setup
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── db/
│   └── schema.sql            # Database table definitions
├── jsons/                    # Sample data files
│   ├── locations.json
│   ├── devices.json
│   └── events.json
├── scripts/
│   ├── database.py           # Database connection manager
│   ├── file_handler.py       # JSON file reader
│   ├── query_runner.py       # Query execution orchestrator
│   ├── importers/            # Data loading classes
│   │   ├── base.py
│   │   ├── locations.py
│   │   ├── devices.py
│   │   └── events.py
│   ├── queries/              # Analytical query classes
│   │   ├── base.py
│   │   ├── leaf_locations.py
│   │   ├── lowest_sublocations.py
│   │   ├── smart_lamp_events.py
│   │   ├── avg_brightness.py
│   │   ├── leak_locations.py
│   │   ├── devices_no_events.py
│   │   └── top_smart_lamp_locations.py
│   └── exporters/            # Output format handlers
│       ├── base.py
│       ├── json_exporter.py
│       └── xml_exporter.py
├── output/                   # Query results (generated)
└── logs/                     # Application logs (generated)
```

## Prerequisites

- Docker and Docker Compose
- Python 3.13+
- pip

## Installation

1. Clone the repository:
```bash
git clone https://github.com/turaljabbarli-innowise/Task1-Python.git
cd Task1-Python
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create `.env` file from template:
```bash
cp .env.example .env
```

5. Start the PostgreSQL database:
```bash
docker-compose up -d
```

## Configuration

Create a `.env` file with the following variables:

```
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## Usage

Run the pipeline with required parameters:

```bash
python run.py --locations jsons/locations.json --devices jsons/devices.json --events jsons/events.json
```

### Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--locations` | Yes | Path to locations JSON file |
| `--devices` | Yes | Path to devices JSON file |
| `--events` | Yes | Path to events JSON file |
| `--format` | No | Output format: `json` or `xml` (default: `xml`) |

### Examples

Export to JSON:
```bash
python run.py --locations jsons/locations.json --devices jsons/devices.json --events jsons/events.json --format json
```

Export to XML:
```bash
python run.py --locations jsons/locations.json --devices jsons/devices.json --events jsons/events.json --format xml
```

## Queries

The pipeline executes the following analytical queries:

1. **Leaf Locations** — Locations without any sublocations
2. **Lowest Sublocations** — Deepest sublocation for each location hierarchy
3. **Smart Lamp Events** — Events where Smart Lamp turned on with brightness > 80
4. **Average Brightness** — Average brightness per location for Smart Lamp on events
5. **Leak Locations** — Locations with devices that detected leaks
6. **Devices No Events** — Devices that have never generated events
7. **Top Smart Lamp Locations** — Top 3 locations by Smart Lamp count

## Output

Results are saved to `output/results.json` or `output/results.xml` depending on the format specified.

Example JSON output structure:
```json
{
  "leaf_locations": [
    {"location_name": "Room A"}
  ],
  "average_brightness": [
    {"location_name": "Room B", "average_brightness": 75.5}
  ]
}
```

## Stopping the Database

```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
```
