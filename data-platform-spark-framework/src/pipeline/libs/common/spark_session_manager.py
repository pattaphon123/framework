"""
Spark Session Manager for Envilink Pipeline Framework.
"""
from pyspark.sql import SparkSession


class SparkSessionManager:
    """Manages Spark session creation and configuration."""
    
    @classmethod
    def get_spark_session(cls, job_class_name: str, pipeline_name: str) -> SparkSession:
        """
        Create and configure Spark session for BigQuery Iceberg.
        
        Args:
            job_class_name: Name of the job class
            pipeline_name: Name of the pipeline
            
        Returns:
            Configured SparkSession instance
        """
        app_name = f"{job_class_name}_{pipeline_name}"
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        return spark

