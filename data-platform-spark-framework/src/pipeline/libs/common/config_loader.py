"""
Configuration loader for Envilink Pipeline Framework.
"""
from typing import Dict, Any, Optional
import yaml
import os
import re
from pathlib import Path
try:
    from importlib.resources import files, as_file
except ImportError:
    # Python < 3.9 fallback
    from importlib_resources import files, as_file
from .gcs_utils import download_file_from_gcs


# Global cache for env_config
_env_config_cache: Optional[Dict[str, Any]] = None


def _load_env_config_from_package() -> Optional[Dict[str, Any]]:
    """
    Load env_config.yaml from the installed package using importlib.resources.
    
    Returns:
        Configuration dictionary if found, None otherwise
    """
    try:
        # Try to load from the installed package
        # This file is in: pipeline.libs.common.config_loader
        # env_config.yaml is in: pipeline/
        package = files('pipeline')
        config_file = package / 'env_config.yaml'
        
        if config_file.is_file():
            # Use as_file to get a real file path for yaml.safe_load
            with as_file(config_file) as path:
                with open(path, 'r') as f:
                    return yaml.safe_load(f) or {}
    except (ModuleNotFoundError, FileNotFoundError, AttributeError):
        # Package not installed or file not found
        pass
    
    return None


def _get_env_config_path() -> Optional[Path]:
    """
    Get the path to env_config.yaml file (for development mode).
    
    Returns:
        Path to env_config.yaml in development environment, or None if not found
    """
    # Try to find the file relative to this file (development mode)
    # This file is in: spark_jobs/src/pipeline/libs/common/config_loader.py
    # env_config.yaml is in: spark_jobs/src/pipeline/env_config.yaml
    current_file = Path(__file__)
    # Navigate: common/ -> libs/ -> pipeline/ -> src/pipeline/
    pipeline_dir = current_file.parent.parent.parent
    config_path = pipeline_dir / 'env_config.yaml'
    
    if config_path.exists():
        return config_path
    
    return None


def load_env_config() -> Dict[str, Any]:
    """
    Load env_config.yaml file.
    
    First tries to load from the installed package, then falls back to
    development mode file path, and finally returns empty dict for backward compatibility.
    
    Returns:
        Environment configuration dictionary
    """
    global _env_config_cache
    
    # Return cached config if available
    if _env_config_cache is not None:
        return _env_config_cache
    
    # Try to load from installed package first (production mode)
    config = _load_env_config_from_package()
    
    if config is None:
        # Fall back to development mode (relative file path)
        env_config_path = _get_env_config_path()
        
        if env_config_path and env_config_path.exists():
            with open(env_config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            # If env_config.yaml doesn't exist, return empty dict
            # This allows backward compatibility
            config = {}
    
    _env_config_cache = config
    return _env_config_cache


def _get_nested_value(config: Dict[str, Any], key_path: str) -> Optional[str]:
    """
    Get nested value from configuration dictionary using dot notation.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated key path (e.g., 'gcp.project_id')
        
    Returns:
        Value at the key path, or None if not found
    """
    keys = key_path.split('.')
    value = config
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    
    return str(value) if value is not None else None


def _substitute_env_placeholders(content: str, env_config: Dict[str, Any]) -> str:
    """
    Substitute ${env.key.path} placeholders with values from env_config.
    
    Args:
        content: YAML content string
        env_config: Environment configuration dictionary
        
    Returns:
        Content with placeholders substituted
    """
    # Pattern to match ${env.key.path} placeholders
    pattern = r'\$\{env\.([^}]+)\}'
    
    def replace_placeholder(match):
        key_path = match.group(1)
        value = _get_nested_value(env_config, key_path)
        
        if value is None:
            # If not found in env_config, try environment variables as fallback
            # Convert key_path to uppercase with underscores (e.g., gcp.project_id -> GCP_PROJECT_ID)
            env_var = key_path.upper().replace('.', '_')
            value = os.getenv(env_var)
            
            if value is None:
                # If still not found, return the original placeholder
                return match.group(0)
        
        return value
    
    return re.sub(pattern, replace_placeholder, content)


def load_config(provider: str, table_name: str, job_type: str) -> Dict[str, Any]:
    """
    Load YAML configuration file from GCS using provider, table name and job type.
    Supports environment variable substitution from env_config.yaml.
    
    Args:
        provider: Data provider (e.g., 'pcd')
        table_name: Name of the table (e.g., 'air4thai')
        job_type: Type of the job (e.g., 'raw_to_discovery', 'discovery_to_standardized')
    """
    # Load env_config.yaml first
    env_config = load_env_config()
    
    config_bucket = _get_nested_value(env_config, 'storage.config_bucket')
    if not config_bucket:
        raise ValueError("GCS config bucket not found in env_config.yaml")

    config_path = f"gs://{config_bucket}/pipelines/refine/{provider}/{table_name}/{job_type}.yaml"
    yaml_content = download_file_from_gcs(config_path)
    
    # First, substitute ${env.key.path} placeholders from env_config.yaml
    expanded_content = _substitute_env_placeholders(yaml_content, env_config)
    
    # Then, expand traditional ${VAR_NAME} environment variables (for backward compatibility)
    expanded_content = os.path.expandvars(expanded_content)
    
    return yaml.safe_load(expanded_content)

