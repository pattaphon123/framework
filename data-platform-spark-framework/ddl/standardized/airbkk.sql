CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized`.`airbkk`
(
    station_id           STRING,
    datetime             TIMESTAMP,
    pm10                 FLOAT64,
    pm25                 FLOAT64,
    co                   FLOAT64,
    no2                  FLOAT64,
    o3                   FLOAT64,
    wind_speed           FLOAT64,
    wind_direction       FLOAT64,
    temperature          FLOAT64,
    relative_humidity    FLOAT64,
    air_pressure         FLOAT64,
    rain                 FLOAT64,
    latitude             FLOAT64,
    longitude            FLOAT64,
    data_date            DATE,
    load_timestamp       TIMESTAMP
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true);
