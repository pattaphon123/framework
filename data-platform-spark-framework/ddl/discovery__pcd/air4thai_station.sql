CREATE NAMESPACE IF NOT EXISTS biglake.discovery__pcd;

CREATE TABLE IF NOT EXISTS biglake.discovery__pcd.air4thai_station
(
    ID                   STRING,
    Name                 STRING,
    NameEn               STRING,
    Area                 STRING,
    AreaEn               STRING,
    OpenDate             STRING,
    CloseDate            STRING,
    data_date            DATE COMMENT 'Partition date',
    load_timestamp       TIMESTAMP COMMENT 'Load timestamp'
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/pcd/air4thai_station'
TBLPROPERTIES ('comment' = 'Raw Air4Thai station metadata');
