-- schema.sql

-- 1. Locations Table
CREATE TABLE IF NOT EXISTS locations (
    location_id VARCHAR(50) PRIMARY KEY,
    parent_location_id VARCHAR(50),
    location_name VARCHAR(100),
    FOREIGN KEY (parent_location_id) REFERENCES locations(location_id)
);

-- 2. Devices Table
CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(50) PRIMARY KEY,
    device_type VARCHAR(50),
    device_name VARCHAR(100),
    location_id VARCHAR(50),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- 3. Events Table
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50),
    timestamp TIMESTAMP,
    details JSONB,
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);
