"""
Load Engine for Envilink Pipeline Framework.
Provides reusable functions for loading data to different table types (Iceberg, BigQuery).
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import time
from google.cloud import bigquery

from ..common.sql_template_renderer import SqlRenderer
from .transform_engine import TransformationEngine
from ...jobs.utils import normalize_table_name, add_technical_columns


class LoadEngine:
    """Engine for loading data to different table types (Iceberg, BigQuery)."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the Load engine.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def build_bigquery_columns_data_types(
        self,
        df: DataFrame,
        columns_config: List[Dict[str, Any]],
        technical_columns_config: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Build BigQuery column data types map from configuration.
        
        Args:
            df: DataFrame containing the data
            columns_config: List of column configurations from target.columns
            technical_columns_config: List of technical column configurations from target.technical_columns
            
        Returns:
            Dictionary mapping column names to BigQuery data types
            
        Raises:
            ValueError: If bigquery_type is missing for any column
        """
        columns_data_types = {}
        all_columns = df.columns
        
        # First, check explicit config columns
        for col_cfg in columns_config:
            name = col_cfg.get('name')
            if name in all_columns:
                bigquery_type = col_cfg.get('bigquery_type')
                if bigquery_type:
                    columns_data_types[name] = bigquery_type
                else:
                    raise ValueError(f"bigquery_type is required for column '{name}' in target.columns configuration")
        
        # Next, check technical columns
        for col_cfg in technical_columns_config:
            name = col_cfg.get('name')
            if name in all_columns:
                bigquery_type = col_cfg.get('bigquery_type')
                if bigquery_type:
                    columns_data_types[name] = bigquery_type
                else:
                    # Default technical column types
                    if name == 'data_date' or name == 'utc_data_date' or name.endswith('_date'):
                        columns_data_types[name] = 'DATE'
                    elif name.endswith('_timestamp') or name.endswith('_ts') or name == 'load_timestamp':
                        columns_data_types[name] = 'TIMESTAMP'
                    elif name == 'is_current':
                        columns_data_types[name] = 'BOOL'
                    else:
                        columns_data_types[name] = 'STRING'
        
        # For any columns still missing types, raise error
        for field in df.schema.fields:
            if field.name not in columns_data_types:
                raise ValueError(
                    f"bigquery_type not found for column '{field.name}'. "
                    f"Please add it to target.columns or target.technical_columns configuration"
                )
        
        return columns_data_types
    
    def prepare_columns(
        self,
        df: DataFrame,
        partition_columns: Optional[List[str]] = None,
        merge_keys: Optional[List[str]] = None
    ) -> tuple[List[str], List[str]]:
        """
        Prepare column lists for loading operations.
        
        Args:
            df: DataFrame containing the data
            partition_columns: List of partition column names (optional)
            merge_keys: List of merge key column names (optional)
            
        Returns:
            Tuple of (select_columns, update_columns):
            - select_columns: Columns for overwrite (excludes partition columns)
            - update_columns: Columns for merge updates (excludes merge keys)
        """
        all_columns = df.columns
        partition_columns = partition_columns or []
        merge_keys = merge_keys or []
        
        # Columns for overwrite (excludes partition columns from select list)
        select_columns = [col for col in all_columns if col not in partition_columns]
        
        # Columns for merge updates (excludes merge keys)
        update_columns = [col for col in all_columns if col not in merge_keys]
        
        return select_columns, update_columns
    
    def prepare_load_context(
        self,
        df: DataFrame,
        target_table: str,
        source_table: str,
        partition_columns: Optional[List[str]] = None,
        merge_keys: Optional[List[str]] = None,
        data_date: Optional[str] = None,
        columns_data_types: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Build context dictionary for SQL template rendering.
        
        Args:
            df: DataFrame containing the data
            target_table: Target table name (normalized)
            source_table: Source table/view name
            partition_columns: List of partition column names (optional)
            merge_keys: List of merge key column names (optional)
            data_date: Data date string (optional)
            columns_data_types: Dictionary mapping column names to BigQuery data types (optional)
            
        Returns:
            Context dictionary for SQL template rendering
        """
        select_columns, update_columns = self.prepare_columns(df, partition_columns, merge_keys)
        
        context = {
            'target_table': target_table,
            'source_table': source_table,
            'columns': select_columns,  # For overwrite: excludes partition columns
            'all_columns': df.columns,  # For merge: all columns including merge keys
            'partition_columns': partition_columns or [],
            'merge_keys': merge_keys or [],
            'update_columns': update_columns,
        }
        
        if data_date:
            context['data_date'] = data_date
        
        if columns_data_types:
            context['columns_data_types'] = columns_data_types
        
        # Extract distinct partition values from DataFrame for dynamic partition overwrite (e.g. BigQuery)
        if partition_columns:
            part_col = partition_columns[0]
            distinct_rows = df.select(part_col).distinct().collect()
            col_type = (columns_data_types or {}).get(part_col, 'STRING')
            formatted_values = []
            for row in distinct_rows:
                val = row[0]
                if val is None:
                    continue
                if col_type in ('DATE', 'TIMESTAMP', 'STRING'):
                    formatted_values.append(f"'{val}'")
                else:
                    formatted_values.append(str(val))
            context['partition_values'] = formatted_values
        
        return context
    
    def _load_to_iceberg(self, sql: str, target_table: str) -> None:
        """
        Execute SQL for Iceberg tables using Spark SQL.
        
        Args:
            sql: SQL statement to execute
            target_table: Target table name (for logging)
        """
        print(sql)
        self.spark.sql(sql)
        print(f"Successfully loaded data to target table: {target_table}")
    
    def _load_to_bigquery(
        self,
        df: DataFrame,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        load_type: str,
        context: Dict[str, Any],
        merge_keys: List[str],
        partition_columns: Optional[List[str]] = None,
        columns_data_types: Dict[str, str] = None
    ) -> None:
        """
        Load data to BigQuery table using BigQuery Client Library.
        
        This handles staging data to a temporary table and then executing the SQL template
        (MERGE, INSERT OVERWRITE, etc.) using the BigQuery client.
        
        Args:
            df: DataFrame containing transformed data
            target_catalog: Target catalog name
            target_schema: Target schema name (BigQuery dataset)
            target_table: Target table name
            load_type: Type of load operation ('merge', 'partition_overwrite', etc.)
            context: Context dictionary for SQL rendering
            merge_keys: List of columns to use for merge (if applicable)
            partition_columns: List of partition columns (if applicable)
            columns_data_types: Dictionary mapping column names to BigQuery data types
        """
        # Use target_catalog as project_id directly (catalog in config now holds project_id for BigQuery)
        if not target_catalog:
            raise ValueError("target_catalog (project_id) is required")
        
        project_id = target_catalog
        
        # Construct BigQuery table references
        # For BigQuery Client, we use project.dataset.table format
        target_bq_table = f"`{project_id}`.`{target_schema}`.`{target_table}`"
        
        # Create staging table name with timestamp to avoid conflicts
        timestamp = int(time.time())
        staging_table_name = f"{target_table}_staging_{timestamp}"
        staging_bq_table = f"`{project_id}`.`{target_schema}`.`{staging_table_name}`"
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        try:
            # Write DataFrame to staging table using BigQuery connector
            print(f"Writing data to staging table: {staging_bq_table}")
            # Spark BigQuery connector expects project:dataset.table or dataset.table
            spark_staging_table = f"{project_id}:{target_schema}.{staging_table_name}"
            
            df.write \
                .format("bigquery") \
                .option("table", spark_staging_table) \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()
            
            print(f"Successfully wrote data to staging table: {staging_bq_table}")
            
            # Update context with staging table as source
            context['source_table'] = staging_bq_table
            context['target_table'] = target_bq_table
            
            # Render SQL based on load_type
            sql = SqlRenderer.render_sql_for_load_type(load_type, context, table_type='bigquery')
            
            print(f"Executing SQL statement:")
            print(sql)
            
            # Execute SQL using BigQuery Client Library
            query_job = client.query(sql)
            query_job.result()  # Wait for the query to complete
            
            print(f"Successfully loaded data to target table: {target_bq_table}")
            
        finally:
            # Clean up staging table
            try:
                # Spark BigQuery connector creates table with project.dataset.table format for reading/writing via API,
                # but for deletion via client we need standard BigQuery format
                staging_table_ref = f"{project_id}.{target_schema}.{staging_table_name}"
                print(f"Dropping staging table: {staging_table_ref}")
                client.delete_table(staging_table_ref, not_found_ok=True)
                print(f"Successfully dropped staging table: {staging_table_ref}")
            except Exception as e:
                print(f"Warning: Failed to drop staging table {staging_table_name}: {str(e)}")
    
    def load(self, df: DataFrame, target_config: Dict[str, Any], data_date: Optional[str]) -> None:
        """
        Orchestrate the entire load process: add technical columns, validate config,
        prepare context, render SQL, and execute load.

        Args:
            df: DataFrame containing transformed data
            target_config: Target configuration dictionary
            data_date: Data date in YYYY-MM-DD format (optional)
        """
        # --- 1. Configuration Extraction ---
        catalog = target_config.get('catalog')
        schema = target_config.get('schema')
        table = target_config.get('table')
        table_type = target_config.get('table_type')
        load_type = 'dynamic_partition_overwrite' if data_date is None else target_config.get('load_type', 'overwrite')

        partition_columns = target_config.get('partition_columns', [])
        technical_columns = target_config.get('technical_columns', [])
        columns_config = target_config.get('columns', [])
        merge_keys = target_config.get('merge_keys', [])

        # --- 2. Technical Columns & Schema Finalization ---
        if technical_columns:
            df = add_technical_columns(df, technical_columns, data_date)
            df = TransformationEngine.select_final_columns(df, columns_config, technical_columns)

        # --- 3. Validation ---
        if not all([catalog, schema, table]):
            raise ValueError("catalog, schema, and table are required in target configuration")
        if not table_type:
            raise ValueError("table_type is required in target configuration")
        if load_type == 'merge' and not merge_keys:
            raise ValueError("merge_keys is required when load_type is 'merge'")
        if table_type == 'bigquery':
            for col_cfg in columns_config:
                if not col_cfg.get('bigquery_type'):
                    raise ValueError(f"bigquery_type is required for column '{col_cfg.get('name')}' for BigQuery")

        # --- 4. Table Normalization ---
        normalized_table_name = normalize_table_name(catalog, schema, table)

        # --- 5. Execution ---
        if table_type == 'bigquery':
            columns_data_types = self.build_bigquery_columns_data_types(df, columns_config, technical_columns)
            context = self.prepare_load_context(
                df, normalized_table_name, '', partition_columns, merge_keys, data_date, columns_data_types
            )
            self._load_to_bigquery(
                df, catalog, schema, table, load_type, context, merge_keys, partition_columns, columns_data_types
            )
        else:
            temp_view = f"temp_target_{catalog}_{schema}_{table}".replace('.', '_')
            df.createOrReplaceTempView(temp_view)
            context = self.prepare_load_context(
                df, normalized_table_name, temp_view, partition_columns, merge_keys, data_date
            )
            sql = SqlRenderer.render_sql_for_load_type(load_type, context, table_type=table_type)

            if load_type == 'dynamic_partition_overwrite':
                self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                try:
                    self._load_to_iceberg(sql, normalized_table_name)
                finally:
                    self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
            else:
                self._load_to_iceberg(sql, normalized_table_name)

