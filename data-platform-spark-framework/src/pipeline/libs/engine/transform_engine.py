"""
Transformation Engine for Envilink Pipeline Framework
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, to_timestamp, regexp_replace,
    trim, lower, upper, substring, coalesce, length, expr, date_format as spark_date_format,
    lpad, explode, explode_outer, struct, array, map_from_arrays, to_json, from_json, concat, when,
    broadcast
)
from pyspark.sql.types import StructType, MapType, ArrayType, StringType, StructField
from typing import List, Dict, Any, Optional
from datetime import datetime


class TransformationEngine:
    """Engine for applying data transformations to DataFrames."""
    
    @staticmethod
    def _transform_trim(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply trim transformation to a column."""
        return df.withColumn(column, trim(col(column)))
    
    @staticmethod
    def _transform_lower(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply lower case transformation to a column."""
        return df.withColumn(column, lower(col(column)))
    
    @staticmethod
    def _transform_upper(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply upper case transformation to a column."""
        return df.withColumn(column, upper(col(column)))
    
    @staticmethod
    def _transform_substring(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply substring transformation to a column."""
        start = transformation.get('start', 0)
        length_val = transformation.get('length', None)
        if length_val:
            return df.withColumn(column, substring(col(column), start + 1, length_val))
        else:
            return df.withColumn(column, substring(col(column), start + 1))
    
    @staticmethod
    def _transform_regexp_replace(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply regexp_replace transformation to a column."""
        pattern = transformation.get('pattern', '')
        replacement = transformation.get('replacement', '')
        return df.withColumn(column, regexp_replace(col(column), pattern, replacement))
    
    @staticmethod
    def _transform_to_date(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """
        Apply to_date transformation to a column.
        
        Supports:
        - ISO 8601 format with 'T' separator (e.g., 'yyyy-MM-dd'T'HH:mm:ss' or 'yyyy-MM-dd')
        - Buddhist Era year conversion (2564 BE = 2021 CE, subtract 543)
        
        Args:
            df: Input DataFrame
            column: Column name to transform
            transformation: Transformation configuration with optional:
                - 'format': Date format pattern (default: 'yyyy-MM-dd')
                - 'convert_buddhist_era': Boolean to convert BE years to CE (default: False)
                - 'from_column': Source column name (default: same as column)
        
        Returns:
            DataFrame with date column transformed
        """
        from_column = transformation.get('from_column', column)
        date_format = transformation.get('format', 'yyyy-MM-dd')
        convert_buddhist_era = transformation.get('convert_buddhist_era', False)
        
        # If Buddhist Era conversion is enabled, convert the year first
        # Buddhist Era year (BE) = Gregorian year (CE) + 543
        # So we subtract 543 from the first 4 digits (year) in the date string
        if convert_buddhist_era:
            # Extract year, convert BE to CE, then replace in the original string
            # Using regexp_replace with REGEXP_EXTRACT to get year, convert, and replace
            # Pattern: ^\\d{4} matches first 4 digits (year) at start of string
            df = df.withColumn(
                column,
                expr(f"regexp_replace(`{from_column}`, '^(\\\\d{{4}})', CAST(CAST(REGEXP_EXTRACT(`{from_column}`, '^(\\\\d{{4}})', 0) AS INT) - 543 AS STRING))")
            )
            from_column = column  # Use the converted column for date parsing
        
        return df.withColumn(column, to_date(col(from_column), date_format))
    
    @staticmethod
    def _transform_to_timestamp(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """
        Apply to_timestamp transformation to a column.
        
        Supports:
        - ISO 8601 format with 'T' separator and timezone offset (e.g., 'yyyy-MM-dd'T'HH:mm:ssXXX')
        - Buddhist Era year conversion (2564 BE = 2021 CE, subtract 543)
        - If the data does not contain timezone information, assumes GMT+7 (Asia/Bangkok) as default.
        
        Args:
            df: Input DataFrame
            column: Column name to transform
            transformation: Transformation configuration with optional:
                - 'format': Timestamp format pattern (default: 'yyyy-MM-dd HH:mm:ss')
                - 'timezone': Timezone for timestamps without timezone info (default: 'Asia/Bangkok')
                - 'convert_buddhist_era': Boolean to convert BE years to CE (default: False)
                - 'from_column': Source column name (default: same as column)
        
        Returns:
            DataFrame with timestamp column transformed
        """
        from_column = transformation.get('from_column', column)
        timestamp_format = transformation.get('format', 'yyyy-MM-dd HH:mm:ss')
        timezone = transformation.get('timezone', 'Asia/Bangkok')  # Default GMT+7
        convert_buddhist_era = transformation.get('convert_buddhist_era', False)
        
        # If Buddhist Era conversion is enabled, convert the year first
        # Buddhist Era year (BE) = Gregorian year (CE) + 543
        # So we subtract 543 from the first 4 digits (year) in the timestamp string
        if convert_buddhist_era:
            # Extract year, convert BE to CE, then replace in the original string
            # Using regexp_replace with REGEXP_EXTRACT to get year, convert, and replace
            # Pattern: ^\\d{4} matches first 4 digits (year) at start of string
            df = df.withColumn(
                column,
                expr(f"regexp_replace(`{from_column}`, '^(\\\\d{{4}})', CAST(CAST(REGEXP_EXTRACT(`{from_column}`, '^(\\\\d{{4}})', 0) AS INT) - 543 AS STRING))")
            )
            from_column = column  # Use the converted column for timestamp parsing
        
        # Check if format includes timezone information
        # Common timezone indicators: 'z', 'Z', 'XXX', 'XX', 'VV', 'V'
        has_timezone_in_format = any(tz_indicator in timestamp_format for tz_indicator in ['z', 'Z', 'XXX', 'XX', 'VV', 'V'])
        
        if has_timezone_in_format:
            # Format includes timezone, parse directly (timezone is in the data)
            return df.withColumn(column, to_timestamp(col(from_column), timestamp_format))
        else:
            # Format doesn't include timezone, assume it's in the specified timezone (default GMT+7)
            # 
            # Parse the timestamp string first (to_timestamp uses session timezone for parsing),
            # then use to_utc_timestamp to treat the parsed timestamp as if it represents
            # a time in the specified timezone (GMT+7) and convert it to UTC.
            #
            # This works correctly because to_utc_timestamp re-interprets the timestamp value
            # in the specified timezone, regardless of how it was originally parsed.
            #
            # Example: "2024-01-01 12:00:00" should be interpreted as "2024-01-01 12:00:00 GMT+7"
            # which equals "2024-01-01 05:00:00 UTC". The to_utc_timestamp function handles this
            # conversion correctly by treating the timestamp as GMT+7 and converting to UTC.
            return df.withColumn(
                column,
                expr(f"to_utc_timestamp(to_timestamp(`{from_column}`, '{timestamp_format}'), '{timezone}')")
            )
    
    @staticmethod
    def _transform_to_time(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply to_time transformation to a column."""
        # Convert time string to time format
        # Format should be like "HHmm" for "1720" -> "17:20:00"
        time_format = transformation.get('format', 'HHmm')
        # Parse the time string and convert to timestamp, then extract time part
        if time_format == 'HHmm':
            # Handle format like "1720" -> "17:20:00"
            # Use backticks for column name in SQL expression
            df = df.withColumn(
                column,
                expr(f"to_timestamp(concat(substring(`{column}`, 1, 2), ':', substring(`{column}`, 3, 2), ':00'), 'HH:mm:ss')")
            )
            return df.withColumn(column, expr(f"date_format(`{column}`, 'HH:mm:ss')"))
        else:
            # For other formats, try to parse with the provided format
            df = df.withColumn(column, to_timestamp(col(column), time_format))
            return df.withColumn(column, expr(f"date_format(`{column}`, 'HH:mm:ss')"))
    
    @staticmethod
    def _transform_combine_date_time(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """
        Combine date and time columns into a timestamp column.

        For time_format 'HHmm', 1- to 3-digit values are zero-padded to 4 digits
        (e.g. "238" -> "0238" as 02:38, "930" -> "0930", "9" -> "0009").
        Integer or string time columns are supported; strings are trimmed.

        Args:
            df: Input DataFrame
            column: Target column name
            transformation: Config with:
                - 'date_column': Name of date column
                - 'time_column': Name of time column
                - 'date_format': Format of the date column (default: 'yyyy-MM-dd')
                - 'time_format': Format of the time column (default: 'HH:mm:ss')
                - 'timezone': Timezone of input data (default: 'Asia/Bangkok')
        """
        date_col = transformation.get('date_column')
        time_col = transformation.get('time_column')
        date_fmt = transformation.get('date_format', 'yyyy-MM-dd')
        time_fmt = transformation.get('time_format', 'HH:mm:ss')
        timezone = transformation.get('timezone', 'Asia/Bangkok')

        if not date_col or not time_col:
            raise ValueError("combine_date_time transformation requires 'date_column' and 'time_column'")

        # Normalize "HHmm" time: pad 1–3 digit values to 4 digits (e.g. "238" -> "0238", "930" -> "0930").
        # Use a temp column so the original time column is not mutated.
        time_expr = time_col
        if time_fmt == 'HHmm':
            _padded_time = '_padded_time_combine_date_time'
            df = df.withColumn(_padded_time, lpad(trim(col(time_col).cast('string')), 4, '0'))
            time_expr = _padded_time

        # Combine formats with space to match the concatenated string
        timestamp_format = f"{date_fmt} {time_fmt}"

        result = df.withColumn(
            column,
            expr(f"to_utc_timestamp(to_timestamp(concat(cast(`{date_col}` as string), ' ', cast(`{time_expr}` as string)), '{timestamp_format}'), '{timezone}')")
        )
        if time_fmt == 'HHmm':
            result = result.drop(_padded_time)
        return result
    
    @staticmethod
    def _transform_date_format(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply date_format transformation to a column."""
        date_format = transformation.get('format', 'yyyy-MM-dd')
        df = df.withColumn(column, col(column).cast('string'))
        # Note: date_format in Spark SQL requires using date_format function
        return df.withColumn(column, spark_date_format(col(column), date_format))
    
    @staticmethod
    def _transform_binning(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """
        Apply binning transformation based on conditions.
        Input column is cast to a number (int/double) for the condition.
        If condition is not met (or cast fails), original value is kept.
        
        Args:
            df: Input DataFrame
            column: Column name
            transformation: Config with:
                - 'rules': List of dicts with 'expr', 'label'
                - 'input_type': 'int' (default) or 'double'
        """
        rules = transformation.get('rules', [])
        input_type_str = transformation.get('input_type', 'int')
        
        # Determine spark type string for casting
        spark_type = "int" if input_type_str == 'int' else "double"
        
        # Cast expression string to replace placeholder
        # We use backticks for safety, and standard SQL CAST
        cast_expr = f"CAST(`{column}` AS {spark_type})"
        
        if not rules:
            return df
            
        # Start building the CASE WHEN expression with the first rule
        first_rule = rules[0]
        
        # --- Build condition for first rule ---
        # Replace {col} placeholder with the cast expression
        rule_expr = first_rule.get('expr', 'false').replace('{col}', cast_expr)
        
        case_expr = when(expr(rule_expr), lit(first_rule.get('label')))
        
        # --- Chain subsequent rules ---
        for rule in rules[1:]:
            rule_expr = rule.get('expr', 'false').replace('{col}', cast_expr)
            case_expr = case_expr.when(expr(rule_expr), lit(rule.get('label')))
            
        # Add otherwise to return original column value (as string)
        case_expr = case_expr.otherwise(col(column))
        
        return df.withColumn(column, case_expr)
    
    @staticmethod
    def _transform_coalesce(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply coalesce transformation to a column."""
        from_columns = transformation.get('from_columns', [column])
        return df.withColumn(column, coalesce(*[col(c) for c in from_columns]))
    
    @staticmethod
    def _transform_static_value(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply static_value transformation to a column."""
        value = transformation.get('value')
        return df.withColumn(column, lit(value))
    
    @staticmethod
    def _transform_to_wkb(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """Apply to_wkb transformation to a column."""
        # Assumes the input column is a WKT string
        # Sedona's ST_GeomFromWKT converts WKT string to geometry
        # ST_AsBinary converts geometry to WKB binary
        return df.withColumn(column, expr(f"ST_AsBinary(ST_GeomFromWKT(`{column}`))"))

    @staticmethod
    def _transform_point_in_polygon(df: DataFrame, column: str, transformation: Dict[str, Any]) -> DataFrame:
        """
        Apply point-in-polygon transformation to populate a specific column.

        Joins the input DataFrame with a boundary table using ST_Intersects on
        latitude/longitude, then sets the target column to the value from the boundary.

        Args:
            df: Input DataFrame
            column: Target column name to populate (e.g., 'country')
            transformation: Config with boundary, latitude_col, longitude_col, boundary_value_col

        Returns:
            DataFrame with target column populated from boundary lookup
        """
        boundary_config = transformation.get('boundary', {})
        boundary_type = boundary_config.get('type', 'iceberg').lower()
        latitude_col = transformation.get('latitude_col', 'latitude')
        longitude_col = transformation.get('longitude_col', 'longitude')
        boundary_geometry_col = boundary_config.get('geometry_col', 'geometry')
        boundary_value_col = transformation.get('boundary_value_col', 'name')
        should_broadcast = transformation.get('broadcast', True)

        spark = df.sparkSession

        catalog = boundary_config.get('catalog')
        schema = boundary_config.get('schema')
        table = boundary_config.get('table')
        if not all([catalog, schema, table]):
            raise ValueError("point_in_polygon requires boundary catalog, schema, and table")

        if boundary_type == 'iceberg':
            boundary_table_name = f"{catalog}.{schema}.{table}"
            boundary_df = spark.read.table(boundary_table_name)
        elif boundary_type == 'bigquery':
            boundary_table_name = f"{catalog}.{schema}.{table}"
            boundary_df = spark.read.format("bigquery").load(boundary_table_name)
        else:
            raise ValueError(
                f"point_in_polygon boundary type must be iceberg or bigquery; got: {boundary_type}"
            )

        boundary_alias = "boundary_df"
        boundary_df = boundary_df.alias(boundary_alias)

        point_expr = (
            f"ST_Point(CAST(`{longitude_col}` AS DECIMAL(24,20)), "
            f"CAST(`{latitude_col}` AS DECIMAL(24,20)))"
        )
        join_condition = expr(
            f"ST_Intersects({point_expr}, "
            f"ST_GeomFromWKT(`{boundary_alias}`.`{boundary_geometry_col}`))"
        )

        if should_broadcast:
            joined_df = df.join(broadcast(boundary_df), join_condition, "left")
        else:
            joined_df = df.join(boundary_df, join_condition, "left")

        ret = joined_df.withColumn(column, col(f"`{boundary_alias}`.`{boundary_value_col}`"))
        return ret.select(*df.columns)

    @staticmethod
    def _transform_dataframe_point_in_polygon(df: DataFrame, transformation: Dict[str, Any]) -> DataFrame:
        """
        Apply point-in-polygon transformation as a DataFrame-level transformation.

        Joins the input DataFrame with a boundary table using ST_Intersects on
        latitude/longitude, and adds specified columns from the boundary table.

        Args:
            df: Input DataFrame
            transformation: Config with boundary, latitude_col, longitude_col, output_columns

        Returns:
            DataFrame with target columns populated from boundary lookup
        """
        boundary_config = transformation.get('boundary', {})
        boundary_type = boundary_config.get('type', 'iceberg').lower()
        latitude_col = transformation.get('latitude_col', 'latitude')
        longitude_col = transformation.get('longitude_col', 'longitude')
        boundary_geometry_col = boundary_config.get('geometry_col', 'geometry')
        
        # Dictionary mapping: {boundary_column: target_column}
        output_columns = transformation.get('output_columns', {})
        should_broadcast = transformation.get('broadcast', True)

        spark = df.sparkSession

        catalog = boundary_config.get('catalog')
        schema = boundary_config.get('schema')
        table = boundary_config.get('table')
        if not all([catalog, schema, table]):
            raise ValueError("point_in_polygon requires boundary catalog, schema, and table")

        if boundary_type == 'iceberg':
            boundary_table_name = f"{catalog}.{schema}.{table}"
            boundary_df = spark.read.table(boundary_table_name)
        elif boundary_type == 'bigquery':
            boundary_table_name = f"{catalog}.{schema}.{table}"
            boundary_df = spark.read.format("bigquery").load(boundary_table_name)
        else:
            raise ValueError(
                f"point_in_polygon boundary type must be iceberg or bigquery; got: {boundary_type}"
            )

        boundary_alias = "boundary_df"
        boundary_df = boundary_df.alias(boundary_alias)

        point_expr = (
            f"ST_Point(CAST(`{longitude_col}` AS DECIMAL(24,20)), "
            f"CAST(`{latitude_col}` AS DECIMAL(24,20)))"
        )
        join_condition = expr(
            f"ST_Intersects({point_expr}, "
            f"ST_GeomFromWKT(`{boundary_alias}`.`{boundary_geometry_col}`))"
        )

        if should_broadcast:
            joined_df = df.join(broadcast(boundary_df), join_condition, "left")
        else:
            joined_df = df.join(boundary_df, join_condition, "left")

        # Determine columns to select
        # 1. Start with original columns, excluding any that will be overwritten by the join output
        target_columns = set(output_columns.values())
        select_exprs = [col(c) for c in df.columns if c not in target_columns]
        
        # 2. Add mapped boundary columns
        for boundary_col, target_col in output_columns.items():
            select_exprs.append(col(f"`{boundary_alias}`.`{boundary_col}`").alias(target_col))

        return joined_df.select(*select_exprs)

    @staticmethod
    def _transform_explode(df: DataFrame, transformation: Dict[str, Any]) -> DataFrame:
        """Apply explode transformation to a DataFrame."""
        col_name = transformation.get('column')
        alias = transformation.get('alias', 'exploded')
        return df.select(col("*"), explode(col(col_name)).alias(alias))
    
    @staticmethod
    def _transform_explode_outer(df: DataFrame, transformation: Dict[str, Any]) -> DataFrame:
        """Apply explode_outer transformation to a DataFrame."""
        col_name = transformation.get('column')
        alias = transformation.get('alias', 'exploded')
        return df.select(col("*"), explode_outer(col(col_name)).alias(alias))
    
    @staticmethod
    def _transform_explode_map(df: DataFrame, transformation: Dict[str, Any]) -> DataFrame:
        """
        Handle explode_map transformation with robust schema unification.
        
        Args:
            df: Input DataFrame
            transformation: Transformation configuration
            
        Returns:
            DataFrame with exploded map
        """
        col_name = transformation.get('column')
        alias = transformation.get('alias', 'exploded_map')
        
        # Resolve data type of the column
        field_schema = df.select(col_name).schema.fields[0]
        data_type = field_schema.dataType
        
        map_col = col(col_name)
        
        if isinstance(data_type, StructType):
            # Convert Struct to Map: keys are field names, values are field values
            # Handle schema mismatch: different stations might have different fields
            
            # 1. Collect all unique fields across all sub-structs to build a "Unified Schema"
            all_fields = set()
            for field in data_type.fields:
                if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    for sub_field in field.dataType.elementType.fields:
                        all_fields.add(sub_field.name)
            
            # 2. Create unified struct schema (all fields as String for maximum compatibility)
            unified_schema = ArrayType(StructType([
                StructField(f_name, StringType(), True) 
                for f_name in sorted(list(all_fields))
            ]))
            
            keys_expr = array([lit(field.name) for field in data_type.fields])
            
            # 3. Normalize all values to the unified schema using JSON roundtrip
            values_list = []
            for field in data_type.fields:
                # Convert to JSON first
                json_col = to_json(col(col_name)[field.name])
                # Parse back with unified schema
                values_list.append(from_json(json_col, unified_schema))
                
            values_expr = array(values_list)
            map_col = map_from_arrays(keys_expr, values_expr)
        
        # Explode map (either original or converted) into key and value columns
        df_exploded = df.select(col("*"), explode(map_col).alias("key", "value"))
        
        # Pack key and value into a struct with the desired alias
        return df_exploded.withColumn(alias, struct(col("key"), col("value"))).drop("key", "value")

    # Dictionary mapping transformation types to their handler methods (column-level)
    _transformation_handlers = {
        'trim': _transform_trim,
        'lower': _transform_lower,
        'upper': _transform_upper,
        'substring': _transform_substring,
        'regexp_replace': _transform_regexp_replace,
        'to_date': _transform_to_date,
        'to_timestamp': _transform_to_timestamp,
        'to_time': _transform_to_time,
        'combine_date_time': _transform_combine_date_time,
        'date_format': _transform_date_format,
        'binning': _transform_binning,
        'coalesce': _transform_coalesce,
        'static_value': _transform_static_value,
        'to_wkb': _transform_to_wkb,
        'point_in_polygon': _transform_point_in_polygon,
    }

    # Dictionary mapping DataFrame-level transformation types to their handler methods
    _dataframe_transformation_handlers = {
        'explode': _transform_explode,
        'explode_map': _transform_explode_map,
        'explode_outer': _transform_explode_outer,
        'point_in_polygon': _transform_dataframe_point_in_polygon,
    }
    
    @staticmethod
    def _has_nested_column(df: DataFrame, column_path: str) -> bool:
        """
        Check if a nested column path exists in the DataFrame schema.
        
        Args:
            df: Input DataFrame
            column_path: Dot-notation path to the column (e.g., 'data.id' or 'parent.child.field')
        
        Returns:
            True if the nested column path exists, False otherwise
        """
        if '.' not in column_path:
            return column_path in df.columns
        
        parts = column_path.split('.')
        root_col = parts[0]
        
        # Check if root column exists
        if root_col not in df.columns:
            return False
        
        # Get the schema of the root column
        root_field = next((f for f in df.schema.fields if f.name == root_col), None)
        if root_field is None:
            return False
        
        # Traverse the nested path
        current_type = root_field.dataType
        for part in parts[1:]:
            if not isinstance(current_type, StructType):
                return False
            
            # Find the field in the struct
            field = next((f for f in current_type.fields if f.name == part), None)
            if field is None:
                return False
            
            current_type = field.dataType
        
        return True
    
    @staticmethod
    def apply_column_transformations(df: DataFrame, schema_config: List[Any]) -> DataFrame:
        """
        Apply all column-level transformations defined in the schema configuration.
        
        Args:
            df: Input DataFrame
            schema_config: List of column definitions (strings or dicts with transformations)
        
        Returns:
            DataFrame with all column-level transformations applied
        """
        for col_def in schema_config:
            # Handle both string and dict column definitions
            if isinstance(col_def, str):
                col_name = col_def
                source_col = col_def
            else:
                col_name = col_def.get('name')
                # Default to target name if source_column not specified
                source_col = col_def.get('source_column', col_name)
            
            transformations = col_def.get('transformations', []) if isinstance(col_def, dict) else []
            
            # Handle column mapping (renaming or extracting nested columns)
            if source_col:
                if source_col != col_name:
                    # Different source and target names
                    if source_col in df.columns:
                        df = df.withColumnRenamed(source_col, col_name)
                    else:
                        # Check if it's a nested column path
                        if '.' in source_col and TransformationEngine._has_nested_column(df, source_col):
                            # Extract nested column value
                            df = df.withColumn(col_name, col(source_col))
                        else:
                            # If source column doesn't exist, create it as null
                            df = df.withColumn(col_name, lit(None).cast('string'))
                else:
                    # Same source and target name, but check if it's nested
                    if source_col not in df.columns:
                        if '.' in source_col and TransformationEngine._has_nested_column(df, source_col):
                            # Extract nested column to top-level with same name
                            df = df.withColumn(col_name, col(source_col))
                        else:
                            # If source column doesn't exist, create it as null
                            df = df.withColumn(col_name, lit(None).cast('string'))
            
            # Apply transformations in order
            for trans in transformations:
                trans_type = trans.get('type')
                handler = TransformationEngine._transformation_handlers.get(trans_type)
                
                if handler:
                    df = handler(df, col_name, trans)
                else:
                    raise ValueError(f"Unsupported transformation type: {trans_type}")
            
            # Always apply final cast based on type field (defaults to 'string')
            if isinstance(col_def, dict):
                target_type = col_def.get('type', 'string')
                df = df.withColumn(col_name, col(col_name).cast(target_type))
        
        return df
    
    @staticmethod
    def apply_dataframe_transformations(df: DataFrame, transformations: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply DataFrame-level transformations (e.g., explode, explode_map, explode_outer).
        
        These transformations change the structure of the DataFrame itself, not individual columns.
        
        Args:
            df: Input DataFrame
            transformations: List of transformation configurations
        
        Returns:
            DataFrame with all DataFrame-level transformations applied
        """
        for transform in transformations:
            transform_type = transform.get('type')
            handler = TransformationEngine._dataframe_transformation_handlers.get(transform_type)
            
            if handler:
                df = handler(df, transform)
            else:
                raise ValueError(f"Unsupported DataFrame-level transformation type: {transform_type}")
        
        return df
    
    @staticmethod
    def select_final_columns(
        df: DataFrame,
        schema_config: List[Any],
        technical_columns: List[Dict[str, Any]],
        cast_type: Optional[str] = None,
    ) -> DataFrame:
        """
        Select only the columns defined in the schema and technical columns.
        
        Args:
            df: Input DataFrame
            schema_config: List of column definitions (strings or dicts with 'name')
            technical_columns: List of technical column definitions from config
            cast_type: If provided, cast schema columns to this type (e.g. 'string')
        
        Returns:
            DataFrame with only final columns (schema + technical columns)
        """
        col_names = [
            c if isinstance(c, str) else c.get('name')
            for c in schema_config
        ]
        technical_col_names = [c.get('name') for c in technical_columns]

        select_exprs = [
            col(name).cast(cast_type) if cast_type else col(name)
            for name in col_names if name in df.columns
        ]
        select_exprs += [
            col(name) for name in technical_col_names if name in df.columns
        ]

        return df.select(*select_exprs)

