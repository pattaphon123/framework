CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized__pcd` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized__pcd`.`air4thai_station`
(
    ID                   STRING,
    Name                 STRING,
    NameEn               STRING,
    Area                 STRING,
    AreaEn               STRING,
    OpenDate             DATE,
    CloseDate            DATE,
    data_date            DATE,
    load_timestamp       TIMESTAMP
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true,
  description="Standardized Air4Thai station master list");
