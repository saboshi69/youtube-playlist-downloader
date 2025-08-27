import os
from typing import List

class Config:
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/app/data/downloads.db')
    DOWNLOAD_DIR = '/downloads/music'
    CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 3600))  # seconds
    DOWNLOAD_FORMAT = os.getenv('DOWNLOAD_FORMAT', 'mp3')
    AUDIO_QUALITY = os.getenv('AUDIO_QUALITY', '320')
    REDIS_URL = 'redis://redis:6379'
    
    # Playlists to monitor (can be configured via API or environment)
    DEFAULT_PLAYLISTS = [
        # Add your playlist URLs here
        'https://music.youtube.com/playlist?list=PL6s8EkICVvvuMANvv8wrVRW91aZful3Xw&si=FR_hb5kM4rdrG0F2',
    ]
