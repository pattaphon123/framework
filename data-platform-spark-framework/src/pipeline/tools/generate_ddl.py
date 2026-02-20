#!/usr/bin/env python3
"""
DDL Generator Script - Simplified version

Generates DDL files from YAML pipeline configurations.
Resolves ${env.*} placeholders from env_config.yaml.

Output structure: ddl/<provider>/<schema>/<table>.sql

Usage:
    python generate_ddl.py --config-dir conf --output-dir ddl
"""

import argparse
import os
import re
import sys
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

# Technical column type mapping
TECHNICAL_COLUMN_TYPES = {
    'data_date': 'DATE',
    'utc_data_date': 'DATE',
    'load_timestamp': 'TIMESTAMP',
    'created_at': 'TIMESTAMP',
    'updated_at': 'TIMESTAMP',
    'ingestion_date': 'DATE',
    'processing_timestamp': 'TIMESTAMP'
}

ENV_VAR_PATTERN = re.compile(r'\$\{env\.([^}]+)\}')


def load_env_config():
    """Load env_config.yaml from pipeline directory."""
    script_dir = Path(__file__).parent
    pipeline_dir = script_dir.parent
    config_path = pipeline_dir / 'env_config.yaml'
    
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}


def get_nested_value(config, key_path):
    """Get nested value from config using dot notation."""
    keys = key_path.split('.')
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return str(value) if value is not None else None


def substitute_placeholders(content, env_config):
    """Substitute ${env.key.path} placeholders in content."""
    def replace(match):
        key_path = match.group(1)
        value = get_nested_value(env_config, key_path)
        if value is None:
            env_var = key_path.upper().replace('.', '_')
            value = os.getenv(env_var)
            if value is None:
                return match.group(0)
        return value
    return ENV_VAR_PATTERN.sub(replace, content)


def substitute_dict_placeholders(obj, env_config):
    """Recursively substitute placeholders in dict/list/string."""
    if isinstance(obj, dict):
        return {k: substitute_dict_placeholders(v, env_config) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [substitute_dict_placeholders(item, env_config) for item in obj]
    elif isinstance(obj, str):
        return substitute_placeholders(obj, env_config)
    return obj


def load_config(config_path):
    """Load YAML config and resolve placeholders."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    env_config = load_env_config()
    return substitute_dict_placeholders(config, env_config)


def infer_column_type(column_name, column_def):
    """Infer column type from definition."""
    if isinstance(column_def, dict):
        if 'bigquery_type' in column_def:
            return column_def['bigquery_type'].upper()
        if 'type' in column_def:
            return column_def['type'].upper()
    if column_name in TECHNICAL_COLUMN_TYPES:
        return TECHNICAL_COLUMN_TYPES[column_name]
    return 'STRING'


def prepare_columns(config):
    """Prepare column definitions."""
    columns = []
    target = config.get('target', {})
    column_defs = target.get('columns', [])
    
    for col_def in column_defs:
        if isinstance(col_def, str):
            col_name = col_def
            col_type = 'STRING'
            col_description = None
        else:
            col_name = col_def.get('alias', col_def.get('name'))
            if 'bigquery_type' in col_def:
                col_type = col_def['bigquery_type'].upper()
            else:
                col_type = col_def.get('type', 'STRING').upper()
            col_description = col_def.get('description')
        
        col_dict = {'name': col_name, 'type': col_type}
        if col_description and isinstance(col_description, str) and col_description.strip():
            col_dict['description'] = col_description.strip()
        columns.append(col_dict)
    
    return columns


def prepare_technical_columns(config):
    """Prepare technical column definitions."""
    technical_columns = []
    target = config.get('target', {})
    tech_col_defs = target.get('technical_columns', [])
    
    for col_def in tech_col_defs:
        if isinstance(col_def, str):
            col_name = col_def
            col_type = infer_column_type(col_name, col_def)
            col_description = None
        else:
            col_name = col_def.get('name')
            col_type = infer_column_type(col_name, col_def)
            col_description = col_def.get('description')
        
        col_dict = {'name': col_name, 'type': col_type}
        if col_description and isinstance(col_description, str) and col_description.strip():
            col_dict['description'] = col_description.strip()
        technical_columns.append(col_dict)
    
    return technical_columns


def generate_ddl(config):
    """Generate DDL SQL from configuration."""
    script_dir = Path(__file__).parent
    templates_dir = script_dir.parent / 'templates'
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        trim_blocks=True,
        lstrip_blocks=True
    )
    env.filters['ljust'] = lambda s, width: str(s).ljust(int(width))
    
    env_config = load_env_config()
    default_region = get_nested_value(env_config, 'gcp.region')
    
    target = config.get('target', {})
    table_type = target.get('table_type', 'iceberg').lower()
    
    if table_type == 'bigquery':
        template = env.get_template('bigquery/create_table.sql.j2')
    else:
        template = env.get_template('iceberg/create_table.sql.j2')
    
    table_description = target.get('description')
    if not (table_description and isinstance(table_description, str) and table_description.strip()):
        table_description = None
    
    context = {
        'pipeline_name': config.get('pipeline_name', 'unknown'),
        'description': config.get('description', ''),
        'table_description': table_description,
        'catalog': target.get('catalog', 'default'),
        'project_id': target.get('catalog', 'default'),
        'schema': target.get('schema', 'default'),
        'table': target.get('table', 'unknown'),
        'table_name': f"{target.get('catalog', 'default')}.{target.get('schema', 'default')}.{target.get('table', 'unknown')}",
        'columns': prepare_columns(config),
        'technical_columns': prepare_technical_columns(config),
        'partition_columns': target.get('partition_columns', []),
        'location': target.get('location', ''),
        'region': target.get('region', default_region)
    }
    
    return template.render(**context)


def generate_ddl_from_directory(config_dir, output_dir):
    """Generate DDL files for all configs in a directory."""
    config_dir = Path(config_dir)
    output_dir = Path(output_dir)
    config_files = list(config_dir.rglob("*.yaml"))
    
    if not config_files:
        print(f"No YAML config files found in: {config_dir}")
        return
    
    print(f"Found {len(config_files)} config file(s)")
    
    for config_file in config_files:
        try:
            config = load_config(config_file)
            
            # Skip files that don't have a target section (e.g., dag_config.yaml)
            if 'target' not in config or not config.get('target'):
                continue
            
            table_name = config.get('target', {}).get('table', 'unknown')
            schema = config.get('target', {}).get('schema', 'discovery')
            provider = config.get('provider', 'unknown')
            
            # Skip if table name is still unknown (invalid config)
            if table_name == 'unknown':
                print(f"Skipping {config_file}: no table name found in target section")
                continue
            
            # Skip if provider is unknown (invalid config)
            if provider == 'unknown':
                print(f"Skipping {config_file}: no provider found in config")
                continue
            
            # Create output directory structure: ddl/<provider>/<schema>/
            output_path = output_dir / provider / schema
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / f"{table_name}.sql"
            
            ddl = generate_ddl(config)
            with open(output_file, 'w') as f:
                f.write(ddl)
            
            print(f"DDL generated: {output_file}")
        except Exception as e:
            print(f"Error processing {config_file}: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Generate DDL files from YAML pipeline configurations')
    parser.add_argument('--config-dir', type=str, default='conf', help='Directory containing YAML config files (default: conf)')
    parser.add_argument('--output-dir', type=str, default='ddl', help='Output directory for DDL files (default: ddl)')
    
    args = parser.parse_args()
    
    try:
        generate_ddl_from_directory(args.config_dir, args.output_dir)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
