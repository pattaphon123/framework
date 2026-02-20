CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized`.`customers`
(
    customer_id          INT64,
    first_name           STRING,
    last_name            STRING,
    email                STRING,
    registration_date    DATE,
    source_system        STRING,
    data_date            DATE,
    load_timestamp       TIMESTAMP
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true);
