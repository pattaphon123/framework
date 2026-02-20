CREATE SCHEMA IF NOT EXISTS `envilink-stg-data-platform`.`standardized` OPTIONS(location="asia-southeast1");

CREATE TABLE IF NOT EXISTS `envilink-stg-data-platform`.`standardized`.`usgs_earthquake`
(
    event_id             STRING OPTIONS(description="Unique event identifier"),
    geometry             GEOGRAPHY OPTIONS(description="Point geometry of the earthquake"),
    magnitude            FLOAT64 OPTIONS(description="Earthquake magnitude"),
    place                STRING OPTIONS(description="Location description"),
    time_epoch_ms        INT64 OPTIONS(description="Event time in epoch milliseconds"),
    updated_epoch_ms     INT64 OPTIONS(description="Last update time in epoch milliseconds"),
    timezone_offset      INT64 OPTIONS(description="Timezone offset in minutes"),
    url                  STRING OPTIONS(description="Link to USGS event page"),
    detail_url           STRING OPTIONS(description="Link to detailed GeoJSON feed"),
    felt_reports         INT64 OPTIONS(description="Number of felt reports"),
    cdi                  FLOAT64 OPTIONS(description="Community Internet Intensity Map (CIIM) intensity"),
    mmi                  FLOAT64 OPTIONS(description="Modified Mercalli Intensity"),
    alert_level          STRING OPTIONS(description="Alert level (green, yellow, orange, red)"),
    status               STRING OPTIONS(description="Event status (automatic vs reviewed)"),
    tsunami_flag         INT64 OPTIONS(description="1 if tsunami warning issued, 0 otherwise"),
    significance         INT64 OPTIONS(description="Significance score (0-1000)"),
    network_code         STRING OPTIONS(description="ID of data contributor"),
    code                 STRING OPTIONS(description="Identifying code"),
    associated_ids       STRING OPTIONS(description="List of event IDs associated with this event"),
    source_networks      STRING OPTIONS(description="List of network codes"),
    product_types        STRING OPTIONS(description="List of product types"),
    nst                  INT64 OPTIONS(description="Number of stations used"),
    distance_min         FLOAT64 OPTIONS(description="Horizontal distance to closest station (degrees)"),
    rms                  FLOAT64 OPTIONS(description="Root Mean Square travel time residual"),
    azimuthal_gap        FLOAT64 OPTIONS(description="Horizontal azimuthal gap (degrees)"),
    magnitude_type       STRING OPTIONS(description="Method used to calculate magnitude (e.g., ml, mb)"),
    event_type           STRING OPTIONS(description="Type of seismic event"),
    title                STRING OPTIONS(description="Full event title"),
    data_date            DATE OPTIONS(description="Partition date"),
    load_timestamp       TIMESTAMP OPTIONS(description="Load timestamp")
)
PARTITION BY data_date
OPTIONS(
  require_partition_filter=true,
  description="Standardized global earthquake data with full field history");
