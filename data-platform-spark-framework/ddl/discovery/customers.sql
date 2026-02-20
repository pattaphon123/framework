CREATE NAMESPACE IF NOT EXISTS biglake.discovery;

CREATE TABLE IF NOT EXISTS biglake.discovery.customers
(
    cust_id              STRING,
    f_name               STRING,
    l_name               STRING,
    email_addr           STRING,
    reg_ts               STRING,
    data_date            DATE,
    load_timestamp       TIMESTAMP
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/customers'
;
