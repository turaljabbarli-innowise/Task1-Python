# IoT Data Pipeline

A Python-based ETL pipeline that loads IoT device data from JSON files into PostgreSQL and executes analytical queries with JSON/XML export.

## Features

- Load hierarchical location data with parent-child relationships
- Import IoT devices and their events
- Store event details as JSONB for flexible querying
- Execute 7 pre-built analytical queries
- Export results to JSON or XML format
- Fully tested with mocked dependencies
- CI/CD pipeline with automated linting and testing

## Project Structure

```
.
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions CI pipeline
├── config.py                 # Environment configuration
├── run.py                    # Main CLI entry point
├── docker-compose.yml        # PostgreSQL container setup
├── requirements.txt          # Python dependencies
├── pytest.ini                # Pytest configuration
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore rules
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
│   │   ├── base.py           # Abstract base importer
│   │   ├── locations.py      # Handles hierarchical location data
│   │   ├── devices.py        # Device data importer
│   │   └── events.py         # Event data importer
│   ├── queries/              # Analytical query classes
│   │   ├── base.py           # Abstract base query
│   │   ├── leaf_locations.py
│   │   ├── lowest_sublocations.py
│   │   ├── smart_lamp_events.py
│   │   ├── avg_brightness.py
│   │   ├── leak_locations.py
│   │   ├── devices_no_events.py
│   │   └── top_smart_lamp_locations.py
│   └── exporters/            # Output format handlers
│       ├── base.py           # Abstract base exporter
│       ├── json_exporter.py
│       └── xml_exporter.py
├── tests/                    # Unit tests
│   ├── conftest.py           # Pytest configuration and fixtures
│   ├── test_file_handler.py
│   ├── test_database.py
│   ├── test_importers.py
│   ├── test_queries.py
│   ├── test_exporters.py
│   └── test_query_runner.py
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
git clone https://github.com/yourusername/iot-data-pipeline.git
cd iot-data-pipeline
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

| Query | Description |
|-------|-------------|
| Leaf Locations | Locations without any sublocations |
| Lowest Sublocations | Deepest sublocation for each location hierarchy |
| Smart Lamp Events | Events where Smart Lamp turned on with brightness > 80 |
| Average Brightness | Average brightness per location for Smart Lamp 'on' events |
| Leak Locations | Locations with devices that detected leaks |
| Devices No Events | Devices that have never generated events |
| Top Smart Lamp Locations | Top 3 locations by Smart Lamp count |

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

## Testing

The project includes comprehensive unit tests using pytest. All tests use mocking to isolate from external dependencies (database, filesystem).

### Running Tests

```bash
pytest
```

With verbose output:
```bash
pytest -v
```

### Test Coverage

| Module | What's Tested |
|--------|---------------|
| FileHandler | JSON parsing, error handling |
| DatabaseManager | Connection, insert, fetch, transactions |
| Importers | Data transformation, hierarchy handling |
| Queries | SQL structure, result mapping |
| Exporters | JSON/XML conversion, file writing |
| QueryRunner | Orchestration logic |

### Testing Approach

Tests use these techniques to avoid real database connections:

- **Mocking**: Replace real objects with fake ones that return controlled values
- **Patching**: Temporarily replace functions/classes during test execution
- **Fixtures**: Reusable test setup shared across tests

Example:
```python
def test_fetch_all_returns_rows(self, connected_db):
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("row1",), ("row2",)]
    # ... test uses fake data, never touches real database
```

## Linting

The project uses flake8 for code style checking.

### Running Linter

```bash
flake8 . --max-line-length=120 --exclude=venv,.venv,__pycache__
```

### What Flake8 Checks

| Code | Meaning |
|------|---------|
| E501 | Line too long |
| W291 | Trailing whitespace |
| W292 | No newline at end of file |
| F401 | Module imported but unused |
| E302 | Expected 2 blank lines |
| E402 | Module import not at top of file |

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration. The pipeline runs automatically on:

- Push to `dev` branch
- Push to `main` branch
- Pull requests targeting `main`

### Pipeline Steps

1. **Lint**: Runs flake8 to check code style
2. **Test**: Installs dependencies and runs pytest

If either step fails, the pipeline fails and the commit is marked as failed.

### Pipeline Configuration

Located at `.github/workflows/ci.yml`:

```yaml
name: CI Pipeline

on:
  push:
    branches: [dev, main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install flake8
      - run: flake8 . --max-line-length=120 --exclude=venv,.venv,__pycache__

  test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install -r requirements.txt
      - run: pytest
```

## Architecture
### Data Flow

```
JSON Files → FileHandler → Importers → PostgreSQL
                                           ↓
                                       Queries
                                           ↓
XML/JSON ← Exporters ← QueryRunner ← Results
```

## Database Schema

```sql
-- Locations (self-referencing for hierarchy)
CREATE TABLE locations (
    location_id VARCHAR(50) PRIMARY KEY,
    parent_location_id VARCHAR(50) REFERENCES locations(location_id),
    location_name VARCHAR(100)
);

-- Devices (linked to locations)
CREATE TABLE devices (
    device_id VARCHAR(50) PRIMARY KEY,
    device_type VARCHAR(50),
    device_name VARCHAR(100),
    location_id VARCHAR(50) REFERENCES locations(location_id)
);

-- Events (JSONB for flexible details)
CREATE TABLE events (
    event_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    timestamp TIMESTAMP,
    details JSONB
);
```

## Stopping the Database

```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
```
