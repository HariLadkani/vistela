-- Database Schema for Videos Table
-- Run this SQL script to create the videos table in your PostgreSQL database

CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    filename VARCHAR(500) NOT NULL,
    s3_key VARCHAR(1000) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on user_id for faster queries
CREATE INDEX IF NOT EXISTS idx_videos_user_id ON videos(user_id);

-- Create index on status for faster filtering
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);

-- Create index on created_at for faster sorting
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at DESC);

-- Optional: Add a trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_videos_updated_at 
    BEFORE UPDATE ON videos 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

