import os
from typing import List

class Config:
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/app/data/downloads.db')
    DOWNLOAD_DIR = '/downloads/music'
    CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 3600))  # seconds
    DOWNLOAD_FORMAT = os.getenv('DOWNLOAD_FORMAT', 'mp3')
    AUDIO_QUALITY = os.getenv('AUDIO_QUALITY', '320')
    REDIS_URL = 'redis://redis:6379'
    
    # Download delay toggle with environment variable
    DOWNLOAD_DELAY_ENABLED = os.getenv('DOWNLOAD_DELAY_ENABLED', 'true').lower() in ['true', '1', 'yes', 'on']
    DOWNLOAD_DELAY_MIN = int(os.getenv('DOWNLOAD_DELAY_MIN', '60'))   # Min delay in seconds
    DOWNLOAD_DELAY_MAX = int(os.getenv('DOWNLOAD_DELAY_MAX', '120'))  # Max delay in seconds
    
    # Playlists to monitor (can be configured via API or environment)
    DEFAULT_PLAYLISTS = [
        # Add your playlist URLs here
    ]
