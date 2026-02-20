"""
Discovery to Standardized PySpark Job
Transforms data from discovery zone to standardized zone using YAML-defined transformations.
"""
from pyspark.sql import DataFrame

from ..libs.engine.transform_engine import TransformationEngine
from ..libs.engine.extract_engine import ExtractEngine
from .base_job import EtlJob


class DiscoveryToStandardizedJob(EtlJob):
    """ETL job for transforming data from discovery to standardized zone."""
    
    def extract(self) -> DataFrame:
        """
        Extract data from discovery table.
        
        The extract phase filters data by data_date to ensure only data for the specified
        date partition is processed. Discovery tables are partitioned by data_date (as the
        first partition), and this filtering ensures data integrity by processing only the
        correct partition even if the table contains multiple date partitions.
        
        Returns:
            DataFrame containing data from discovery zone filtered by data_date
        """
        extract_engine = ExtractEngine(self.spark)
        df = extract_engine.extract_from_source(self.source, self.data_date, self.target)
        
        # Only drop existing technical columns if data_date is provided (single-date mode)
        # In dynamic mode (data_date is None), we keep them to preserve data_date from discovery
        if self.data_date is not None:
            technical_columns_config = self.target.get('technical_columns', [])
            tech_col_names = [c.get('name') for c in technical_columns_config]
            existing_tech_cols = [c for c in tech_col_names if c in df.columns]
            if existing_tech_cols:
                df = df.drop(*existing_tech_cols)
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform data from discovery to standardized format.
        
        Args:
            df: DataFrame containing data from discovery zone
            
        Returns:
            DataFrame transformed for standardized zone
        """
        # Apply source (DataFrame-level) transformations if configured (e.g. point_in_polygon)
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

        # Apply column transformations
        df_standardized = TransformationEngine.apply_column_transformations(df, schema_config)
        
        # Select both schema and technical columns
        technical_columns = self.target.get('technical_columns', [])
        df_standardized = TransformationEngine.select_final_columns(
            df_standardized,
            schema_config,
            technical_columns,
        )
        
        return df_standardized
    
