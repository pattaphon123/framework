"""
Google Cloud Storage utilities for Envilink Pipeline Framework.
"""
from urllib.parse import urlparse
from google.cloud import storage
from typing import Tuple


def parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """
    Parse a GCS path into bucket name and blob name.
    
    Args:
        gcs_path: GCS path in format gs://bucket/path/to/file
        
    Returns:
        Tuple of (bucket_name, blob_name)
    """
    parsed = urlparse(gcs_path)
    bucket_name = parsed.netloc
    blob_name = parsed.path.lstrip('/')
    return bucket_name, blob_name


def download_file_from_gcs(gcs_path: str) -> str:
    """
    Download a file from Google Cloud Storage and return its content as text.
    
    Args:
        gcs_path: GCS path in format gs://bucket/path/to/file.yaml
        
    Returns:
        File content as string
    """
    bucket_name, blob_name = parse_gcs_path(gcs_path)
    
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Download and return content
    return blob.download_as_text()

