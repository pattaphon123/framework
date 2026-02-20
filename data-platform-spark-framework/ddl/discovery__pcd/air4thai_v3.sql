CREATE NAMESPACE IF NOT EXISTS biglake.discovery__pcd;

CREATE TABLE IF NOT EXISTS biglake.discovery__pcd.air4thai_v3
(
    stationID            STRING COMMENT 'Station Identifier',
    DATETIMEDATA         STRING COMMENT 'Measurement timestamp (Raw)',
    PM25                 STRING,
    PM10                 STRING,
    O3                   STRING,
    CO                   STRING,
    NO2                  STRING,
    SO2                  STRING,
    WS                   STRING,
    WD                   STRING,
    TEMP                 STRING,
    RH                   STRING,
    BP                   STRING,
    RAIN                 STRING,
    data_date            DATE COMMENT 'Partition date from file path',
    load_timestamp       TIMESTAMP COMMENT 'System load timestamp'
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/pcd/air4thai_v3'
;
