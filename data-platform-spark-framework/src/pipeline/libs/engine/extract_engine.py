"""
Extract Engine for Envilink Pipeline Framework.
Provides reusable functions for extracting data from various sources (files, tables).
"""
import re
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, expr, input_file_name, length, lit, regexp_extract, to_date, when

from ...jobs.utils import filter_by_data_date, normalize_table_name


class ExtractEngine:
    """Engine for extracting data from different source types (files, tables)."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the Extract engine.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def extract_from_source(self, source_config: Dict[str, Any], data_date: Optional[str], target_config: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Extract data from source based on source configuration.
        
        Routes to appropriate extraction method based on source type.
        
        Args:
            source_config: Source configuration dictionary
            data_date: Data date in YYYY-MM-DD format (optional)
            target_config: Target configuration dictionary (optional, used to detect partition column)
            
        Returns:
            DataFrame containing extracted data
        """
        source_type = source_config.get('type', 'gcs')
        
        if source_type == 'gcs':
            return self._extract_from_file(source_config, data_date, target_config)
        elif source_type in ['table', 'iceberg', 'bigquery']:
            return self._extract_from_table(source_config, data_date)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _extract_from_file(self, source_config: Dict[str, Any], data_date: Optional[str], target_config: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Extract data from file-based source (GCS).
        
        Supports multiple file formats: csv, json, geojson, parquet, shapefile, and geopackage.
        The 'options' configuration parameter is passed directly to the Spark DataFrameReader,
        allowing format-specific options (e.g., mergeSchema for parquet, multiLine for json).
        
        Handles path-based filtering by replacing {{ ds }} and {{ ds_nodash }} placeholders
        and applies column-based filtering by data_date if the column exists.
        When data_date is None and placeholders appear anywhere in the path (directory or filename),
        reads all matching partitions/files and adds a date partition column derived from the file path.
        The date column name is determined from target_config partition_columns (defaults to 'data_date').
        
        Args:
            source_config: Source configuration dictionary with keys:
                - format: File format ('csv', 'json', 'geojson', 'parquet', 'shapefile', or 'geopackage')
                - path: Source path (supports {{ ds }} and {{ ds_nodash }} placeholders)
                - options: Optional dictionary of format-specific reader options
            data_date: Data date in YYYY-MM-DD format (optional)
            target_config: Target configuration dictionary (optional, used to detect partition column name)
            
        Returns:
            DataFrame containing extracted data filtered by date partition column (or with date from path)
        """
        source_format = source_config.get('format', 'csv')
        path = source_config.get('path', '')
        source_options = source_config.get('options', {})
        
        # Determine which date column to use from target partition_columns
        # Priority: utc_data_date > data_date (default to data_date if not specified)
        date_column_name = 'data_date'  # default
        if target_config:
            partition_columns = target_config.get('partition_columns', [])
            if 'utc_data_date' in partition_columns:
                date_column_name = 'utc_data_date'
            elif 'data_date' in partition_columns:
                date_column_name = 'data_date'

        # When data_date is provided: single-partition read (replace placeholders with value)
        # When data_date is None and path has placeholders: multi-partition read (replace with *)
        if data_date:
            data_date_nodash = data_date.replace('-', '')
            source_path = path.replace('{{ ds }}', data_date).replace('{{ ds_nodash }}', data_date_nodash)
            extract_date_from_path = False
        else:
            has_placeholder = '{{ ds }}' in path or '{{ ds_nodash }}' in path
            if has_placeholder:
                source_path = path.replace('{{ ds }}', '*').replace('{{ ds_nodash }}', '*')
                # Extract data_date from path (directory or filename) via input_file_name() regex
                extract_date_from_path = True
            else:
                source_path = path
                extract_date_from_path = False

        print(f"Extracting data from {source_format} file: {source_path}")

        if source_format == 'csv':
            df = self.spark.read.options(**source_options).csv(source_path)
        elif source_format == 'json':
            df = self.spark.read.options(**source_options).json(source_path)
        elif source_format == 'geojson':
            df = self._read_geojson(source_path, source_options)
        elif source_format == 'parquet':
            df = self.spark.read.options(**source_options).parquet(source_path)
        elif source_format == 'shapefile':
            df = self.spark.read.format("shapefile").options(**source_options).load(source_path)
        elif source_format == 'geopackage':
            df = self.spark.read.format("geopackage").options(**source_options).load(source_path)
        else:
            raise ValueError(f"Unsupported format: {source_format}")

        if extract_date_from_path:
            # Build regex from path: escape then replace placeholders with date capture groups.
            # Prepend .* so the pattern matches the full URI returned by input_file_name() (e.g. gs://bucket/...).
            # Append .* so the pattern matches paths with date in directory (e.g. .../20260130/file.json)
            # or in filename (e.g. .../data-20260130.json). Replace escaped * (glob) with .* for wildcards.
            regex_pattern = re.escape(path)
            regex_pattern = regex_pattern.replace(re.escape('{{ ds }}'), r'(\d{4}-\d{2}-\d{2})')
            regex_pattern = regex_pattern.replace(re.escape('{{ ds_nodash }}'), r'(\d{8})')
            regex_pattern = regex_pattern.replace(re.escape('*'), r'.*')
            regex_pattern = r'.*' + regex_pattern + r'.*'
            extracted = regexp_extract(input_file_name(), regex_pattern, 1)
            # Cast to date: YYYY-MM-DD (length 10) or YYYYMMDD (length 8)
            # Use the date column name from target partition_columns
            df = df.withColumn(
                date_column_name,
                when(length(extracted) == 10, to_date(extracted, 'yyyy-MM-dd')).otherwise(
                    to_date(extracted, 'yyyyMMdd')
                )
            )

        # When data_date arg is None (dynamic mode), ensure date column exists for downstream load
        # Use the date column name from target partition_columns
        if data_date is None and date_column_name not in df.columns:
            df = df.withColumn(date_column_name, lit(None).cast('date'))

        df = filter_by_data_date(df, data_date)
        return df
    
    def _extract_from_table(self, source_config: Dict[str, Any], data_date: Optional[str]) -> DataFrame:
        """
        Extract data from table-based source (Iceberg, BigQuery).
        
        Reads from a table and filters by data_date partition column if it exists.
        
        Args:
            source_config: Source configuration dictionary
            data_date: Data date in YYYY-MM-DD format (optional)
            
        Returns:
            DataFrame containing extracted data filtered by data_date
        """
        source_catalog = source_config.get('catalog')
        source_schema = source_config.get('schema')
        source_table = source_config.get('table')
        
        if not all([source_catalog, source_schema, source_table]):
            raise ValueError("catalog, schema, and table are required in source configuration")
        
        # Normalize table name
        normalized_table_name = normalize_table_name(source_catalog, source_schema, source_table)
        
        # Read from table
        df = self.spark.read.table(normalized_table_name)
        
        # Filter by data_date partition column to ensure only the correct date is processed
        # This is critical for partitioned tables that may contain multiple date partitions
        df = filter_by_data_date(df, data_date)
        
        return df
    
    def _read_geojson(self, source_path: str, source_options: dict) -> DataFrame:
        """
        Read GeoJSON file and extract features into a flattened DataFrame.
        
        Args:
            source_path: Path to GeoJSON file
            source_options: Additional options for reading
        
        Returns:
            DataFrame with flattened GeoJSON features
        """
        # Read the GeoJSON file as JSON
        df_raw = self.spark.read.options(**source_options).format("geojson").load(source_path)
        
        # Explode the features array
        df_features = df_raw.select(explode(col("features")).alias("feature"))
        
        # Convert geometry JSON to string for Sedona ST_GeomFromGeoJSON
        # Sedona's ST_GeomFromGeoJSON expects a JSON string
        df_with_geom_str = df_features.withColumn(
            "geometry_json",
            expr("feature.geometry")
        )
        
        # Use Sedona to convert GeoJSON to geometry, then to WKT
        # ST_GeomFromGeoJSON converts GeoJSON string to geometry
        # ST_AsText converts geometry to WKT (Well-Known Text) string
        df_flattened = df_with_geom_str.select(
            expr("ST_AsText(geometry_json)").alias("geometry"),
            col("feature.properties.*")
        )
        
        return df_flattened

