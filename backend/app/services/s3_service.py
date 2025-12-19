"""
S3 Service

This module provides functionality for uploading files to AWS S3.
Uses boto3 for S3 operations and python-dotenv for environment variable management.
"""

import os
import logging
from typing import BinaryIO, Optional
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# AWS Configuration from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Configure boto3 with retry settings
# This helps handle transient errors automatically
boto3_config = Config(
    retries={
        'max_attempts': 3,  # Maximum number of retry attempts
        'mode': 'adaptive'  # Adaptive retry mode adjusts retry timing
    }
)


def get_s3_client():
    """
    Create and return a configured S3 client.
    
    Returns:
        boto3.client: Configured S3 client
        
    Raises:
        ValueError: If AWS credentials are not set in environment variables
    """
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in environment variables"
        )
    
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME must be set in environment variables")
    
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=boto3_config
    )


def upload_to_s3(file: BinaryIO, filename: str, folder: Optional[str] = None) -> str:
    """
    Upload a file to S3 bucket.
    
    This function handles file uploads to S3 with automatic retries for transient errors.
    For more complex retry logic, you can wrap this function with exponential backoff
    using libraries like tenacity or backoff.
    
    Args:
        file: File-like object (BinaryIO) to upload
        filename: Name of the file (will be used as S3 object key)
        folder: Optional folder/prefix to organize files in S3 (e.g., "uploads/", "images/")
                If provided, the object key will be "{folder}/{filename}"
    
    Returns:
        str: The S3 object key (path) of the uploaded file
        
    Raises:
        ValueError: If AWS credentials or bucket name are not configured
        ClientError: If S3 operation fails (e.g., bucket doesn't exist, permission denied)
        BotoCoreError: If there's a low-level error with boto3
        
    Example:
        with open("image.jpg", "rb") as f:
            s3_key = upload_to_s3(f, "image.jpg", folder="uploads")
            print(f"File uploaded to: {s3_key}")
    """
    try:
        s3_client = get_s3_client()
        
        # Construct the S3 object key
        if folder:
            # Remove leading/trailing slashes from folder and add one at the end
            folder = folder.strip("/")
            object_key = f"{folder}/{filename}"
        else:
            object_key = filename
        
        # Reset file pointer to beginning in case it was read before
        file.seek(0)
        
        # Upload file to S3
        # The boto3 client will automatically retry on transient errors based on our config
        s3_client.upload_fileobj(
            file,
            S3_BUCKET_NAME,
            object_key,
            # Optional: Add extra arguments here for metadata, ACL, etc.
            # ExtraArgs={
            #     'ContentType': 'image/jpeg',
            #     'Metadata': {'custom-key': 'custom-value'}
            # }
        )
        
        logger.info(f"Successfully uploaded {filename} to S3 bucket {S3_BUCKET_NAME} as {object_key}")
        
        return object_key
        
    except ValueError as e:
        # Configuration errors - don't retry, just log and re-raise
        logger.error(f"Configuration error: {e}")
        raise
        
    except ClientError as e:
        # S3-specific errors (e.g., bucket not found, access denied, etc.)
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        
        logger.error(f"S3 ClientError during upload: {error_code} - {error_message}")
        
        # For certain errors, you might want to implement custom retry logic:
        # - 500, 502, 503, 504: Server errors - retry with exponential backoff
        # - 403: Access denied - don't retry, check permissions
        # - 404: Bucket not found - don't retry, check bucket name
        # - 400: Bad request - don't retry, check file/parameters
        
        # Example: For server errors, you could implement exponential backoff here
        # if error_code in ['500', '502', '503', '504']:
        #     # Implement exponential backoff retry logic
        #     pass
        
        raise
        
    except BotoCoreError as e:
        # Low-level boto3 errors (network issues, etc.)
        logger.error(f"BotoCoreError during upload: {e}")
        
        # Network errors are often transient and can be retried
        # The boto3 config already handles basic retries, but for more control,
        # you could implement exponential backoff here using libraries like:
        # - tenacity: @retry decorator with exponential backoff
        # - backoff: @backoff.on_exception decorator
        
        raise
        
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"Unexpected error during S3 upload: {e}")
        raise


# Example usage with retry decorator (commented out - requires additional dependency):
"""
To implement more sophisticated retry logic, you can use the tenacity library:

1. Add to requirements.txt: tenacity>=8.0.0

2. Decorate the function:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, BotoCoreError)),
        reraise=True
    )
    def upload_to_s3(file: BinaryIO, filename: str, folder: Optional[str] = None) -> str:
        # ... existing code ...
"""

