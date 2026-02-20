CREATE NAMESPACE IF NOT EXISTS biglake.discovery;

CREATE TABLE IF NOT EXISTS biglake.discovery.usgs_earthquake
(
    id                   STRING COMMENT 'Unique event identifier',
    geometry             STRING COMMENT 'WKT Geometry',
    mag                  STRING COMMENT 'Magnitude',
    place                STRING COMMENT 'Location description',
    time                 STRING COMMENT 'Event time in epoch milliseconds',
    updated              STRING COMMENT 'Update time in epoch milliseconds',
    tz                   STRING COMMENT 'Timezone offset',
    url                  STRING COMMENT 'USGS Event Page URL',
    detail               STRING COMMENT 'Link to GeoJSON detail feed',
    felt                 STRING COMMENT 'Number of felt reports',
    cdi                  STRING COMMENT 'Community Internet Intensity Map (CIIM) intensity',
    mmi                  STRING COMMENT 'Modified Mercalli Intensity',
    alert                STRING COMMENT 'Alert level (green, yellow, orange, red)',
    status               STRING COMMENT 'Event status (automatic/reviewed)',
    tsunami              STRING COMMENT 'Tsunami warning flag (0 or 1)',
    sig                  STRING COMMENT 'Significance score',
    net                  STRING COMMENT 'ID of data contributor',
    code                 STRING COMMENT 'Identifying code',
    ids                  STRING COMMENT 'List of event IDs associated with this event',
    sources              STRING COMMENT 'List of network codes',
    types                STRING COMMENT 'List of product types',
    nst                  STRING COMMENT 'Number of stations used',
    dmin                 STRING COMMENT 'Horizontal distance to closest station',
    rms                  STRING COMMENT 'Root Mean Square travel time residual',
    gap                  STRING COMMENT 'Horizontal azimuthal gap',
    magType              STRING COMMENT 'Magnitude type (e.g., ml, md)',
    type                 STRING COMMENT 'Type of seismic event',
    title                STRING COMMENT 'Descriptive title',
    data_date            DATE COMMENT 'Partition date',
    load_timestamp       TIMESTAMP COMMENT 'Load timestamp'
)
USING iceberg
PARTITIONED BY (data_date)
LOCATION 'gs://envilink-asia-southeast1-stg-discovery/usgs_earthquake'
TBLPROPERTIES ('comment' = 'Raw USGS earthquake data');
