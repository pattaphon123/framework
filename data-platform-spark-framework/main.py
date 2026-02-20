"""
Standalone main entry point for Dataproc
This file is uploaded to GCS and used as the main_python_file_uri in Dataproc jobs.
It imports from the envilink_spark package which is installed from the wheel file.
"""
from src.main import main

if __name__ == '__main__':
    main()

