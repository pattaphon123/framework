"""
Base ETL Job class for Envilink Pipeline Framework
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import sys
from pyspark.sql import SparkSession, DataFrame

from ..libs.common.config_loader import load_config
from ..libs.common.spark_session_manager import SparkSessionManager
from ..libs.engine.dq_engine import DQEngine
from ..libs.engine.load_engine import LoadEngine
from .utils import normalize_table_name


class EtlJob(ABC):
    """Abstract base class for all ETL jobs."""
    
    def __init__(self, provider: str, table_name: str, job_type: str, data_date: Optional[str] = None, spark: Optional[SparkSession] = None):
        """
        Initialize the ETL job.
        
        Args:
            provider: Data provider (e.g., 'pcd')
            table_name: Name of the table to process (e.g., 'air4thai')
            job_type: Type of the job (e.g., 'raw_to_discovery', 'discovery_to_standardized')
            data_date: Data date in YYYY-MM-DD format (optional; omit for multi-partition / dynamic partition overwrite)
            spark: Optional SparkSession instance. If provided, uses this session;
                   otherwise creates a new one via SparkSessionManager
        """
        self.provider = provider
        self.table_name = table_name
        self.job_type = job_type
        self.data_date = data_date
        self.config = load_config(provider, table_name, job_type)
        self.pipeline_name = self.config.get('pipeline_name', 'unknown')
        self.source = self.config.get('source', {})
        self.target = self.config.get('target', {})
        if spark is not None:
            self.spark = spark
        else:
            self.spark = SparkSessionManager.get_spark_session(self.__class__.__name__, self.pipeline_name)
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from the source.
        
        This method must be implemented by subclasses.
        
        Returns:
            DataFrame containing extracted data
        """
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the extracted data.
        
        This method must be implemented by subclasses.
        
        Args:
            df: DataFrame containing extracted data
            
        Returns:
            DataFrame containing transformed data
        """
        pass
    
    def quality_check(self, df: DataFrame) -> DataFrame:
        """
        Perform data quality checks on the transformed data.
        
        Default implementation that executes quality checks from target configuration.
        Subclasses can override if custom behavior is needed.
        
        Args:
            df: DataFrame containing transformed data
            
        Returns:
            DataFrame after quality checks
        """
        # Execute data quality checks
        quality_checks = self.target.get('quality_checks', [])
        if quality_checks:
            dq_engine = DQEngine(self.spark)
            dq_engine.execute_all_checks(df, quality_checks)
        
        return df
    
    def load(self, df: DataFrame) -> None:
        """
        Load the transformed data to the target.
        
        Default implementation that loads data using LoadEngine.
        Subclasses can override if custom behavior is needed.
        
        Args:
            df: DataFrame containing transformed data
        """
        load_engine = LoadEngine(self.spark)
        load_engine.load(df, self.target, self.data_date)
    
    def execute(self) -> None:
        """
        Execute the job with proper error handling and cleanup.
        
        This is the main entry point that should be called externally.
        It orchestrates the extract, transform, quality_check, and load phases.
        """
        try:
            extracted_df = self.extract()
            transformed_df = self.transform(extracted_df)
            quality_checked_df = self.quality_check(transformed_df)
            self.load(quality_checked_df)
        except Exception as e:
            print(f"Error in {self.__class__.__name__}: {str(e)}", file=sys.stderr)
            raise
        finally:
            self.spark.stop()

