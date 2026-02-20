CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized__pcd` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized__pcd`.`air4thai_v3`
(
    stationID            STRING OPTIONS(description="Station Identifier"),
    DATETIMEDATA         TIMESTAMP OPTIONS(description="Date and time when the measurement was taken"),
    PM25                 FLOAT64 OPTIONS(description="Particulate matter 2.5 micrometers or smaller in diameter (μg/m³)"),
    PM10                 FLOAT64 OPTIONS(description="Particulate matter 10 micrometers or smaller in diameter (μg/m³)"),
    O3                   FLOAT64 OPTIONS(description="Ozone concentration (ppb)"),
    CO                   FLOAT64 OPTIONS(description="Carbon monoxide concentration (ppm)"),
    NO2                  FLOAT64 OPTIONS(description="Nitrogen dioxide concentration (ppb)"),
    SO2                  FLOAT64 OPTIONS(description="Sulfur dioxide concentration (ppb)"),
    WS                   FLOAT64 OPTIONS(description="Wind speed (m/s)"),
    WD                   FLOAT64 OPTIONS(description="Wind direction in degrees (0-360)"),
    TEMP                 FLOAT64 OPTIONS(description="Air temperature in Celsius"),
    RH                   FLOAT64 OPTIONS(description="Relative humidity percentage (0-100)"),
    BP                   FLOAT64 OPTIONS(description="Barometric pressure (hPa)"),
    RAIN                 FLOAT64 OPTIONS(description="Rainfall amount (mm)"),
    data_date            DATE OPTIONS(description="Partition date for the data record"),
    load_timestamp       TIMESTAMP OPTIONS(description="Timestamp when the record was loaded into the table")
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true);
