"""
Database Service

This module provides functions to interact with AWS RDS PostgreSQL database.
Uses psycopg2 for direct SQL queries and python-dotenv for environment variable management.
"""

import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool, sql
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
backend_dir = Path(__file__).resolve().parent.parent.parent
env_path = backend_dir / ".env"
load_dotenv(dotenv_path=env_path)

# Configure logging
logger = logging.getLogger(__name__)

# Database Configuration from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connection pool (optional - for better performance with multiple requests)
# Uncomment to use connection pooling:
# connection_pool = psycopg2.pool.SimpleConnectionPool(
#     1, 20,  # min and max connections
#     host=DB_HOST,
#     user=DB_USER,
#     password=DB_PASS,
#     database=DB_NAME,
#     port=DB_PORT
# )


def get_db_connection():
    """
    Create and return a database connection.
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        ValueError: If database credentials are not set in environment variables
        psycopg2.OperationalError: If connection to database fails
    """
    if not all([DB_HOST, DB_USER, DB_PASS, DB_NAME]):
        raise ValueError(
            "DB_HOST, DB_USER, DB_PASS, and DB_NAME must be set in environment variables"
        )
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def insert_video(video_id: str, user_id: str, filename: str, s3_key: str, status: str = "pending") -> bool:
    """
    Insert a new video record into the database.
    
    Args:
        video_id: Unique identifier for the video
        user_id: Identifier for the user who uploaded the video
        filename: Original filename of the video
        s3_key: S3 object key where the video is stored
        status: Status of the video (default: "pending")
                Common statuses: "pending", "processing", "completed", "failed"
    
    Returns:
        bool: True if insert was successful, False otherwise
        
    Raises:
        ValueError: If database credentials are not configured
        psycopg2.IntegrityError: If video_id already exists (unique constraint violation)
        psycopg2.Error: For other database errors
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # SQL query to insert video record
        # Note: Ensure your videos table has these columns:
        # CREATE TABLE videos (
        #     video_id VARCHAR PRIMARY KEY,
        #     user_id VARCHAR NOT NULL,
        #     filename VARCHAR NOT NULL,
        #     s3_key VARCHAR NOT NULL,
        #     status VARCHAR NOT NULL DEFAULT 'pending',
        #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        # );
        
        insert_query = """
            INSERT INTO videos (video_id, user_id, filename, s3_key, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        current_time = datetime.utcnow()
        cursor.execute(
            insert_query,
            (video_id, user_id, filename, s3_key, status, current_time, current_time)
        )
        
        conn.commit()
        logger.info(f"Successfully inserted video {video_id} for user {user_id}")
        return True
        
    except psycopg2.IntegrityError as e:
        conn.rollback()
        logger.error(f"Integrity error inserting video {video_id}: {e}")
        raise
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error inserting video {video_id}: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error inserting video {video_id}: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_video(video_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a video record by video_id.
    
    Args:
        video_id: Unique identifier for the video
    
    Returns:
        dict: Video record as a dictionary, or None if not found
              Keys: video_id, user_id, filename, s3_key, status, created_at, updated_at
        
    Raises:
        ValueError: If database credentials are not configured
        psycopg2.Error: For database errors
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # SQL query to get video by ID
        select_query = """
            SELECT video_id, user_id, filename, s3_key, status, created_at, updated_at
            FROM videos
            WHERE video_id = %s
        """
        
        cursor.execute(select_query, (video_id,))
        result = cursor.fetchone()
        
        if result:
            # Convert to regular dict (RealDictCursor returns OrderedDict)
            return dict(result)
        return None
        
    except psycopg2.Error as e:
        logger.error(f"Database error getting video {video_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting video {video_id}: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


def list_videos(user_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """
    List videos with optional filtering by user_id and status.
    
    Args:
        user_id: Optional filter by user_id
        status: Optional filter by status (e.g., "pending", "completed")
        limit: Maximum number of records to return (default: 100)
    
    Returns:
        list: List of video records as dictionaries
              Each dict contains: video_id, user_id, filename, s3_key, status, created_at, updated_at
        
    Raises:
        ValueError: If database credentials are not configured
        psycopg2.Error: For database errors
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build dynamic query based on filters
        base_query = """
            SELECT video_id, user_id, filename, s3_key, status, created_at, updated_at
            FROM videos
            WHERE 1=1
        """
        
        params = []
        
        if user_id:
            base_query += " AND user_id = %s"
            params.append(user_id)
        
        if status:
            base_query += " AND status = %s"
            params.append(status)
        
        base_query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Convert to list of dicts
        return [dict(row) for row in results]
        
    except psycopg2.Error as e:
        logger.error(f"Database error listing videos: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error listing videos: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


# ============================================================================
# TODO: Add more database queries below
# ============================================================================

# Example: Update video status
# def update_video_status(video_id: str, status: str) -> bool:
#     """
#     Update the status of a video.
#     
#     Args:
#         video_id: Unique identifier for the video
#         status: New status value
#     
#     Returns:
#         bool: True if update was successful
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         
#         update_query = """
#             UPDATE videos
#             SET status = %s, updated_at = %s
#             WHERE video_id = %s
#         """
#         
#         cursor.execute(update_query, (status, datetime.utcnow(), video_id))
#         conn.commit()
#         return cursor.rowcount > 0
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

# Example: Delete video
# def delete_video(video_id: str) -> bool:
#     """
#     Delete a video record from the database.
#     
#     Args:
#         video_id: Unique identifier for the video
#     
#     Returns:
#         bool: True if deletion was successful
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         
#         delete_query = "DELETE FROM videos WHERE video_id = %s"
#         cursor.execute(delete_query, (video_id,))
#         conn.commit()
#         return cursor.rowcount > 0
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

# Example: Get videos by user
# def get_videos_by_user(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """
#     Get all videos for a specific user.
#     
#     Args:
#         user_id: User identifier
#         limit: Maximum number of records to return
#     
#     Returns:
#         list: List of video records
#     """
#     return list_videos(user_id=user_id, limit=limit)

# Example: Get video statistics
# def get_video_stats(user_id: Optional[str] = None) -> Dict[str, int]:
#     """
#     Get statistics about videos (count by status).
#     
#     Args:
#         user_id: Optional filter by user_id
#     
#     Returns:
#         dict: Statistics with counts by status
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor(cursor_factory=RealDictCursor)
#         
#         query = """
#             SELECT status, COUNT(*) as count
#             FROM videos
#             WHERE (%s IS NULL OR user_id = %s)
#             GROUP BY status
#         """
#         
#         cursor.execute(query, (user_id, user_id))
#         results = cursor.fetchall()
#         
#         return {row['status']: row['count'] for row in results}
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

