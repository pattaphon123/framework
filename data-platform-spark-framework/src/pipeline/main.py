"""
Main entry point for Envilink Spark Jobs
Dynamically selects and executes the appropriate ETL job based on command-line arguments.
"""
import argparse
import sys
from pipeline.jobs.raw_to_discovery import RawToDiscoveryJob
from pipeline.jobs.discovery_to_standardized import DiscoveryToStandardizedJob


def main():
    """Main entry point for all ETL jobs."""
    parser = argparse.ArgumentParser(description='Envilink ETL Job Runner')
    parser.add_argument('--job-name', required=True, 
                       choices=['raw_to_discovery', 'discovery_to_standardized'],
                       help='Name of the job to execute')
    parser.add_argument('--table', required=True, 
                       help='Name of the table to process (e.g., air4thai)')
    parser.add_argument('--provider', required=True,
                       help='Data provider (e.g., pcd, nasa)')
    parser.add_argument('--data-date', required=False, default=None,
                       help='Data date in YYYY-MM-DD format (optional; omit for multi-partition / dynamic partition overwrite)')
    
    args = parser.parse_args()
    
    # Map job names to job classes
    job_classes = {
        'raw_to_discovery': RawToDiscoveryJob,
        'discovery_to_standardized': DiscoveryToStandardizedJob,
    }
    
    job_class = job_classes[args.job_name]
    
    # Create and execute the job
    try:
        job = job_class(args.provider, args.table, args.job_name, args.data_date)
        job.execute()
        print(f"Job '{args.job_name}' completed successfully")
    except Exception as e:
        print(f"Job '{args.job_name}' failed: {str(e)}", file=sys.stderr)
        raise


if __name__ == '__main__':
    main()
