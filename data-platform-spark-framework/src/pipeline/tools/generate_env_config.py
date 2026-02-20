#!/usr/bin/env python3
"""
Environment Configuration Generator - Simplified version

Generates env_config.yaml file from environment variables.
Use --env-file to load from a .env file (for local development).

Usage:
    python generate_env_config.py
    python generate_env_config.py --env-file .env
    python generate_env_config.py --output /path/to/env_config.yaml
"""

import argparse
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

REQUIRED_ENV_VARS = ['GCP_PROJECT_ID', 'GCP_REGION', 'RAW_BUCKET', 'BIGLAKE_CATALOG']


def generate_config(env_file=None):
    """Generate configuration dictionary from environment variables."""
    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")
        load_dotenv(env_path, override=True)
    
    # Check required variables
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Get required values
    gcp_project_id = os.getenv('GCP_PROJECT_ID')
    gcp_region = os.getenv('GCP_REGION')
    raw_bucket = os.getenv('RAW_BUCKET')
    biglake_catalog = os.getenv('BIGLAKE_CATALOG')
    
    # Get optional values
    gcp_zone = os.getenv('GCP_ZONE')
    staging_bucket = os.getenv('STAGING_BUCKET')
    config_bucket = os.getenv('CONFIG_BUCKET')
    discovery_bucket = os.getenv('DISCOVERY_BUCKET', raw_bucket)
    standardized_bucket = os.getenv('STANDARDIZED_BUCKET', staging_bucket if staging_bucket else raw_bucket)
    
    # Build config
    gcp_config = {'project_id': gcp_project_id, 'region': gcp_region}
    if gcp_zone:
        gcp_config['zone'] = gcp_zone
    
    storage_config = {'raw_bucket': raw_bucket}
    if staging_bucket:
        storage_config['staging_bucket'] = staging_bucket
    if config_bucket:
        storage_config['config_bucket'] = config_bucket
    storage_config['discovery_bucket'] = discovery_bucket
    storage_config['standardized_bucket'] = standardized_bucket
    
    return {
        'gcp': gcp_config,
        'storage': storage_config,
        'biglake': {'catalog': biglake_catalog}
    }


def write_config(config, output_path):
    """Write configuration to YAML file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2)
    print(f"Generated env_config.yaml at: {output_path}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Generate env_config.yaml from environment variables')
    parser.add_argument('--output', type=str, help='Output path for env_config.yaml')
    parser.add_argument('--env-file', type=str, help='Path to .env file to load environment variables from')
    
    args = parser.parse_args()
    
    try:
        config = generate_config(args.env_file)
        
        if args.output:
            output_file = Path(args.output)
        else:
            script_dir = Path(__file__).parent
            pipeline_dir = script_dir.parent
            output_file = pipeline_dir / 'env_config.yaml'
        
        write_config(config, output_file)
        print(f"\nConfiguration generated successfully!")
        print(f"   Location: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
