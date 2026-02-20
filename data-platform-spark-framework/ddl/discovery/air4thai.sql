CREATE NAMESPACE IF NOT EXISTS biglake.discovery;

CREATE TABLE IF NOT EXISTS biglake.discovery.air4thai
(
    stationID            STRING COMMENT 'Unique identifier for the air quality monitoring station',
    DATETIMEDATA         STRING COMMENT 'Date and time of the measurement in string format',
    PM25                 STRING COMMENT 'Particulate matter 2.5 micrometers or smaller (raw string value)',
    PM10                 STRING COMMENT 'Particulate matter 10 micrometers or smaller (raw string value)',
    O3                   STRING COMMENT 'Ozone concentration (raw string value)',
    CO                   STRING COMMENT 'Carbon monoxide concentration (raw string value)',
    NO2                  STRING COMMENT 'Nitrogen dioxide concentration (raw string value)',
    SO2                  STRING COMMENT 'Sulfur dioxide concentration (raw string value)',
    WS                   STRING COMMENT 'Wind speed (raw string value)',
    WD                   STRING COMMENT 'Wind direction (raw string value)',
    TEMP                 STRING COMMENT 'Temperature (raw string value)',
    RH                   STRING COMMENT 'Relative humidity (raw string value)',
    BP                   STRING COMMENT 'Barometric pressure (raw string value)',
    RAIN                 STRING COMMENT 'Rainfall amount (raw string value)',
    data_date            DATE COMMENT 'Partition date for the data record',
    load_timestamp       TIMESTAMP COMMENT 'Timestamp when the record was loaded into the discovery zone'
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/air4thai'
TBLPROPERTIES ('comment' = 'Raw Air4Thai air quality monitoring data ingested from JSON source files. All columns are stored as strings in the discovery zone.');
