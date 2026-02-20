"""
Raw to Discovery PySpark Job
Ingests raw data and loads it into the discovery zone as Iceberg tables in BigQuery.
"""
from pyspark.sql import DataFrame

from ..libs.engine.transform_engine import TransformationEngine
from ..libs.engine.extract_engine import ExtractEngine
from .base_job import EtlJob


class RawToDiscoveryJob(EtlJob):
    """ETL job for ingesting raw data into the discovery zone."""
    
    def extract(self) -> DataFrame:
        """
        Extract raw data from the source.
        
        The extract phase filters data by data_date in two ways:
        1. Path-based filtering: The data_date is incorporated into the source_path
           by replacing {{ ds }} or {{ ds_nodash }} placeholders, ensuring only data for the specified
           date is read from partitioned storage (e.g., GCS paths like gs://bucket/data/{{ ds }}/file.csv
           or gs://bucket/data/{{ ds_nodash }}/file.csv).
        2. Column-based filtering: After reading, if a 'data_date' column exists in the DataFrame,
           an additional filter is applied to ensure only rows matching self.data_date are included.
           This provides an extra layer of data integrity for sources that may contain multiple dates.
        
        Returns:
            DataFrame containing raw data filtered by data_date
        """
        extract_engine = ExtractEngine(self.spark)
        return extract_engine.extract_from_source(self.source, self.data_date, self.target)
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform raw data for discovery zone.
        
        Args:
            df: DataFrame containing raw data
            
        Returns:
            DataFrame transformed for discovery zone
        """
        # Apply source transformations if configured
        # This allows handling nested structures like arrays (explode) before standardization
        transformations = self.source.get('transformations', [])
        if transformations:
            dataframe_transformations = [
                t for t in transformations 
                if t.get('type') in TransformationEngine._dataframe_transformation_handlers
            ]
            if dataframe_transformations:
                df = TransformationEngine.apply_dataframe_transformations(df, dataframe_transformations)
            
        # Get target columns configuration
        schema_config = self.target.get('columns', [])
        
        # Apply column renaming and transformations
        df = TransformationEngine.apply_column_transformations(df, schema_config)
        
        # Select configured columns (cast to string for discovery convention)
        # and preserve technical columns (e.g. data_date) added during extraction
        technical_columns = self.target.get('technical_columns', [])
        return TransformationEngine.select_final_columns(
            df, schema_config, technical_columns, cast_type='string'
        )
    
