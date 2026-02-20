"""
SQL Renderer for Envilink Pipeline Framework.
"""
import importlib.resources
from jinja2 import Template
from typing import Dict, Any, Optional


class SqlRenderer:
    """Renders Jinja SQL templates from package resources."""
    
    @staticmethod
    def _load_template(template_path: str) -> str:
        """
        Load a Jinja template from package resources.
        
        Args:
            template_path: Path to the template file (e.g., 'iceberg/merge.sql.j2' or 'bigquery/merge.sql.j2')
            
        Returns:
            Template content as string
        """
        try:
            # Python 3.9+ way
            template_file = importlib.resources.files('pipeline.templates') / template_path
            return template_file.read_text(encoding='utf-8')
        except (AttributeError, TypeError):
            # Fallback for older Python versions
            import importlib.resources as pkg_resources
            return pkg_resources.read_text('pipeline.templates', template_path, encoding='utf-8')
    
    @staticmethod
    def render_template(template_path: str, context: Dict[str, Any]) -> str:
        """
        Render a Jinja SQL template with the given context.
        
        Args:
            template_path: Path to the template file (e.g., 'iceberg/merge.sql.j2')
            context: Dictionary of variables to render the template with
            
        Returns:
            Rendered SQL statement
        """
        template_text = SqlRenderer._load_template(template_path)
        template = Template(template_text)
        return template.render(**context)
    
    @staticmethod
    def _get_template_path(table_type: str, load_type: str) -> str:
        """
        Get the template path based on table_type and load_type.
        
        Args:
            table_type: The table type ('iceberg' or 'bigquery')
            load_type: The load type ('merge', 'partition_overwrite', 'full_dump')
            
        Returns:
            Template path (e.g., 'iceberg/merge.sql.j2')
        """
        # Map load_type to template filename
        template_map = {
            'merge': 'merge.sql.j2',
            'partition_overwrite': 'partition_overwrite.sql.j2',
            'dynamic_partition_overwrite': 'dynamic_partition_overwrite.sql.j2',
            'full_dump': 'full_dump.sql.j2',
        }
        
        template_filename = template_map.get(load_type)
        if not template_filename:
            raise ValueError(f"Unsupported load_type: {load_type}. Use 'merge', 'partition_overwrite', or 'full_dump'")
        
        return f"{table_type}/{template_filename}"
    
    @staticmethod
    def render_sql_for_load_type(load_type: str, context: Dict[str, Any], table_type: Optional[str] = None) -> str:
        """
        Render SQL based on load_type and table_type using the appropriate template.
        
        Args:
            load_type: The load type ('partition_overwrite', 'full_dump', 'merge')
            context: Dictionary of variables to render the template with
            table_type: The table type ('iceberg' or 'bigquery'). Must be provided.
            
        Returns:
            Rendered SQL statement
        """
        # Validate table_type is provided
        if table_type is None:
            raise ValueError("table_type is required. Use 'iceberg' or 'bigquery'")
        
        # Validate table_type
        if table_type not in ['iceberg', 'bigquery']:
            raise ValueError(f"Unsupported table_type: {table_type}. Use 'iceberg' or 'bigquery'")
        
        # Get template path
        template_path = SqlRenderer._get_template_path(table_type, load_type)
        
        # For merge, use all_columns if available, otherwise use columns
        render_context = context.copy()
        if load_type == 'merge' and 'all_columns' in render_context:
            render_context['columns'] = render_context['all_columns']
        
        return SqlRenderer.render_template(template_path, render_context)

