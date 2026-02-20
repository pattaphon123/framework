CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized`.`air4thai`
(
    station_id           STRING OPTIONS(description="Unique identifier for the air quality monitoring station"),
    datetime_data        TIMESTAMP OPTIONS(description="Date and time when the measurement was taken"),
    pm25                 FLOAT64 OPTIONS(description="Particulate matter 2.5 micrometers or smaller in diameter (μg/m³)"),
    pm10                 FLOAT64 OPTIONS(description="Particulate matter 10 micrometers or smaller in diameter (μg/m³)"),
    o3                   FLOAT64 OPTIONS(description="Ozone concentration (ppb)"),
    co                   FLOAT64 OPTIONS(description="Carbon monoxide concentration (ppm)"),
    no2                  FLOAT64 OPTIONS(description="Nitrogen dioxide concentration (ppb)"),
    so2                  FLOAT64 OPTIONS(description="Sulfur dioxide concentration (ppb)"),
    wind_speed           FLOAT64 OPTIONS(description="Wind speed (m/s)"),
    wind_direction       FLOAT64 OPTIONS(description="Wind direction in degrees (0-360)"),
    temperature          FLOAT64 OPTIONS(description="Air temperature in Celsius"),
    relative_humidity    FLOAT64 OPTIONS(description="Relative humidity percentage (0-100)"),
    barometric_pressure  FLOAT64 OPTIONS(description="Barometric pressure (hPa)"),
    rainfall             FLOAT64 OPTIONS(description="Rainfall amount (mm)"),
    data_date            DATE OPTIONS(description="Partition date for the data record"),
    load_timestamp       TIMESTAMP OPTIONS(description="Timestamp when the record was loaded into the table")
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true,
  description="Standardized Air4Thai air quality monitoring data with cleansed and validated measurements from monitoring stations across Thailand.");
