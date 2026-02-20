"""
Data Quality (DQ) engine for Envilink Pipeline Framework
Uses Great Expectations (GX 1.0+ API) for data validation
"""
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Any, Optional
import logging
import uuid

# Import Great Expectations
try:
    import great_expectations as gx
    from great_expectations.core.expectation_validation_result import ExpectationValidationResult
except ImportError:
    print("Warning: great_expectations package not found.")
    gx = None
    ExpectationValidationResult = None

logger = logging.getLogger(__name__)

class DQEngine:
    """Engine for executing data quality checks using Great Expectations."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the DQ engine.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        if gx is None:
            raise ImportError("Great Expectations not installed.")
        
        # Get the Ephemeral Data Context (fast, in-memory, no config files needed)
        self.context = gx.get_context(mode="ephemeral")
    
    def _add_not_null_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a not_null expectation to the suite."""
        column = check_config.get('column')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column, meta=meta))
    
    def _add_unique_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a unique expectation to the suite (requires columns list)."""
        columns = check_config.get('columns')
        if not columns:
            raise ValueError("'columns' list is required for 'unique' check")
        suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=columns, meta=meta))
    
    def _add_min_length_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a min_length expectation to the suite."""
        column = check_config.get('column')
        length = check_config.get('length')
        suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToBeBetween(column=column, min_value=length, meta=meta))
    
    def _add_max_length_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a max_length expectation to the suite."""
        column = check_config.get('column')
        length = check_config.get('length')
        suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToBeBetween(column=column, max_value=length, meta=meta))
    
    def _add_value_in_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a value_in expectation to the suite."""
        column = check_config.get('column')
        values = check_config.get('values')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column=column, value_set=values, meta=meta))
    
    def _add_regexp_match_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a regexp_match expectation to the suite."""
        column = check_config.get('column')
        pattern = check_config.get('pattern')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column=column, regex=pattern, meta=meta))
    
    def _add_min_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a min value expectation to the suite."""
        column = check_config.get('column')
        min_value = check_config.get('value')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=column, min_value=min_value, meta=meta))
    
    def _add_max_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a max value expectation to the suite."""
        column = check_config.get('column')
        max_value = check_config.get('value')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=column, max_value=max_value, meta=meta))
    
    def _add_value_in_range_expectation(self, suite, check_config: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """Add a value_in_range expectation to the suite."""
        column = check_config.get('column')
        min_value = check_config.get('min_value')
        max_value = check_config.get('max_value')
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=min_value,
            max_value=max_value,
            meta=meta
        ))
    
    # Dictionary mapping check types to their handler methods
    _check_handlers = {
        'not_null': _add_not_null_expectation,
        'unique': _add_unique_expectation,
        'min_length': _add_min_length_expectation,
        'max_length': _add_max_length_expectation,
        'value_in': _add_value_in_expectation,
        'regexp_match': _add_regexp_match_expectation,
        'min': _add_min_expectation,
        'max': _add_max_expectation,
        'value_in_range': _add_value_in_range_expectation,
    }

    def execute_all_checks(self, df: DataFrame, checks: List[Dict[str, Any]]) -> None:
        """
        [OPTIMIZED]
        Executes all DQ checks by building ONE suite and calling validate() ONE time.
        """
        if not checks:
            logger.info("No DQ checks to execute")
            return

        logger.info(f"Preparing {len(checks)} DQ checks for a single validation run...")

        # --- 1. GX 1.0 SETUP (ONCE) ---
        unique_id = str(uuid.uuid4())[:8]
        ds_name = f"spark_ds_{unique_id}"
        asset_name = f"asset_{unique_id}"
        suite_name = f"suite_{unique_id}"

        try:
            datasource = self.context.data_sources.add_spark(name=ds_name)
            asset = datasource.add_dataframe_asset(name=asset_name)
            batch_def = asset.add_batch_definition_whole_dataframe("default_batch_def")
            batch = batch_def.get_batch(batch_parameters={"dataframe": df})
            suite = self.context.suites.add(gx.ExpectationSuite(name=suite_name))

        except Exception as e:
            logger.error(f"Failed during GX setup: {e}", exc_info=True)
            raise

        # --- 2. POPULATE SUITE (IN A LOOP) ---
        for idx, check_config in enumerate(checks, 1):
            check_type = check_config.get('type')
            column = check_config.get('column')
            columns = check_config.get('columns')
            on_fail = check_config.get('on_fail', 'FAIL')
            
            # Pass our config to 'meta' so we can retrieve 'on_fail' from the results
            meta = {'on_fail': on_fail, 'check_type': check_type, 'column': column, 'columns': columns}
            
            # Log column(s) appropriately
            if columns:
                column_str = f"columns={columns}"
            else:
                column_str = f"column='{column}'"
            logger.info(f"Adding check {idx}/{len(checks)}: type='{check_type}', {column_str}, on_fail='{on_fail}'")
            
            try:
                handler = DQEngine._check_handlers.get(check_type)
                if handler:
                    handler(self, suite, check_config, meta)
                else:
                    logger.warning(f"Unsupported check type: {check_type}. Skipping.")
            
            except Exception as e:
                # Format column identifier for error message
                if columns:
                    col_identifier = f"columns {columns}"
                else:
                    col_identifier = f"column '{column}'"
                logger.error(f"Failed to add check {check_type} on {col_identifier}: {e}", exc_info=True)


        # --- 3. VALIDATE (ONCE) ---
        logger.info(f"Executing validation on suite '{suite_name}' with {len(suite.expectations)} checks...")
        validation_result = batch.validate(suite)
        results: List[ExpectationValidationResult] = validation_result.results


        # --- 4. PROCESS & PRINT RESULTS ---
        passed_checks_count = 0
        failed_check_reports = [] # Stores formatted string reports for failed checks
        has_critical_failure = False

        for res in results:
            # Extract meta from the config
            meta = res.expectation_config.meta or {}
            
            # Use 'check_type' from meta, but fallback to 'type' from expectation_config
            check_type = meta.get('check_type', res.expectation_config.type)

            column = meta.get('column', 'N/A')
            columns = meta.get('columns')
            severity = meta.get('on_fail', 'FAIL')
            
            # Format column identifier for logging
            if columns:
                column_identifier = f"columns {columns}"
            else:
                column_identifier = f"column '{column}'"

            if res.success:
                passed_checks_count += 1
                logger.info(f"✓ DQ check '{check_type}' on {column_identifier} passed")
            else:
                # Build a detailed error report
                result_details = res.result
                unexpected_count = result_details.get('unexpected_count', 'N/A')
                unexpected_percent = result_details.get('unexpected_percent', 0.0)
                element_count = result_details.get('element_count', 'N/A')
                
                # Basic error message
                error_msg = f"Found {unexpected_count} unexpected values ({unexpected_percent:.2f}%) out of {element_count} rows."
                
                # Add specific details if available
                if 'partial_unexpected_counts' in result_details and result_details['partial_unexpected_counts']:
                    counts_str = ", ".join([
                        f"'{item['value']}' ({item['count']} times)" 
                        for item in result_details['partial_unexpected_counts']
                    ])
                    error_msg += f" Duplicates: [{counts_str}]"
                elif 'partial_unexpected_list' in result_details and result_details['partial_unexpected_list']:
                    list_str = ", ".join([f"'{v}'" for v in result_details['partial_unexpected_list']])
                    error_msg += f" Examples: [{list_str}]"
                
                # Format the final report string for this failure
                report_lines = []
                report_lines.append(f"Check:    {check_type} on {column_identifier}")
                report_lines.append(f"   Severity: {severity}")
                report_lines.append(f"   Error:    {error_msg}")
                failed_check_reports.append("\n".join(report_lines))

                if severity == 'FAIL':
                    has_critical_failure = True
                    logger.error(f"✗ DQ check '{check_type}' on {column_identifier} failed (FAIL): {error_msg}")
                else: # 'WARN'
                    logger.warning(f"⚠ DQ check '{check_type}' on {column_identifier} failed (WARN): {error_msg}")
        
        # --- [NEW] PRINT SUMMARY SECTION ---
        print("\n" + "="*60)
        print("           DATA QUALITY (DQ) CHECK SUMMARY")
        print("="*60)
        print(f"\nTotal Checks Executed: {len(results)}")
        print(f"Checks Passed: {passed_checks_count}")
        print(f"Checks Failed: {len(failed_check_reports)}")
        
        if failed_check_reports:
            print("\n--- FAILED CHECK DETAILS ---")
            for i, report_str in enumerate(failed_check_reports, 1):
                print(f"\n{i}. {report_str}")
        
        print("\n" + "="*60 + "\n")
        # --- [END] PRINT SUMMARY SECTION ---

        # --- 5. HANDLE JOB FAILURE ---
        if has_critical_failure:
            logger.error("Job failed due to one or more critical (on_fail: FAIL) DQ checks.")
            raise ValueError("Job failed due to critical DQ check failures. See summary above.")
        
        logger.info(f"All {len(results)} DQ checks completed.")
        if failed_check_reports:
             logger.warning("Some non-critical (on_fail: WARN) DQ checks failed but job will continue.")

