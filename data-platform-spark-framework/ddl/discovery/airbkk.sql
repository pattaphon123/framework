CREATE NAMESPACE IF NOT EXISTS biglake.discovery;

CREATE TABLE IF NOT EXISTS biglake.discovery.airbkk
(
    station_id           STRING,
    Date_Time            STRING,
    PM10                 STRING,
    PM2_5                STRING,
    CO                   STRING,
    NO2                  STRING,
    O3                   STRING,
    WS                   STRING,
    WD                   STRING,
    Temp                 STRING,
    RH                   STRING,
    BP                   STRING,
    RAIN                 STRING,
    Latitude             STRING,
    Longitude            STRING,
    data_date            DATE,
    load_timestamp       TIMESTAMP
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/airbkk'
;
