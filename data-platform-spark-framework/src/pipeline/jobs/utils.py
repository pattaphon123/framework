"""
Shared utilities for ETL jobs.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, current_timestamp, lit, col


def normalize_table_name(catalog: str, schema: str, table: str) -> str:
    """
    Normalize table name by constructing it from catalog, schema, and table components.
    
    Args:
        catalog: Catalog name (e.g., 'biglake')
        schema: Schema name (e.g., 'discovery')
        table: Table name (e.g., 'customers')
        
    Returns:
        Normalized table name in format catalog.schema.table
    """
    return f"{catalog}.{schema}.{table}"


def add_technical_columns(df: DataFrame, technical_columns: List[Dict[str, Any]], data_date: Optional[str]) -> DataFrame:
    """
    Add technical columns to a DataFrame based on configuration.
    Column values are inferred from the column name.
    
    Args:
        df: Input DataFrame
        technical_columns: List of technical column definitions from config (with 'name' field)
        data_date: Data date in YYYY-MM-DD format (optional)
    
    Returns:
        DataFrame with technical columns added
    """
    date_expr = to_date(lit(data_date), 'yyyy-MM-dd') if data_date else None

    column_expressions = {
        'data_date': date_expr,
        'utc_data_date': date_expr,
        'load_timestamp': current_timestamp(),
        'effective_start_date': date_expr,
        'effective_end_date': to_date(lit('9999-12-31'), 'yyyy-MM-dd'),
        'is_current': lit(True),
    }

    for col_def in technical_columns:
        col_name = col_def.get('name')
        if col_name in df.columns:
            continue
        expr = column_expressions.get(col_name, date_expr)
        if expr is not None:
            df = df.withColumn(col_name, expr)

    return df


def filter_by_data_date(df: DataFrame, data_date: Optional[str]) -> DataFrame:
    """
    Filter DataFrame by date partition column with priority-based selection.
    
    This function filters data by date partition column to ensure only data for the specified
    date is processed. It handles both string and date column types by casting the column to
    date for robust comparison.
    
    Priority order:
    1. If data_date parameter is None → no filtering
    2. If 'utc_data_date' column exists → filter by utc_data_date
    3. Else if 'data_date' column exists → filter by data_date
    4. Else → no filtering (column doesn't exist)
    
    Args:
        df: Input DataFrame
        data_date: Data date in YYYY-MM-DD format. If None, no filtering is applied.
        
    Returns:
        DataFrame filtered by date partition column (or original DataFrame if no date column exists)
    """
    if data_date is None:
        return df

    data_date_lit = to_date(lit(data_date), 'yyyy-MM-dd')
    
    # Priority 1: utc_data_date
    if 'utc_data_date' in df.columns:
        df = df.filter(col('utc_data_date').cast('date') == data_date_lit)
    # Priority 2: data_date
    elif 'data_date' in df.columns:
        df = df.filter(col('data_date').cast('date') == data_date_lit)
    # Priority 3: No filtering (column doesn't exist)
    
    return df

