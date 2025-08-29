import sqlite3
import json
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.init_database()

    def init_database(self):
        """Initialize database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS playlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    name TEXT,
                    last_checked TIMESTAMP,
                    active BOOLEAN DEFAULT 1,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT UNIQUE NOT NULL,
                    title TEXT,
                    uploader TEXT,
                    duration INTEGER,
                    upload_date TEXT,
                    playlist_id INTEGER,
                    file_path TEXT,
                    metadata TEXT,
                    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_hash TEXT,
                    file_size INTEGER,
                    status TEXT DEFAULT 'pending',
                    FOREIGN KEY (playlist_id) REFERENCES playlists (id)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS download_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT,
                    playlist_id INTEGER,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    details TEXT,
                    error_message TEXT,
                    FOREIGN KEY (playlist_id) REFERENCES playlists (id)
                )
            ''')

    def add_playlist(self, url: str, name: str = None):
        """Add a new playlist to monitor"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute(
                    'INSERT INTO playlists (url, name) VALUES (?, ?)',
                    (url, name)
                )
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                cursor = conn.execute('SELECT id FROM playlists WHERE url = ?', (url,))
                result = cursor.fetchone()
                return result[0] if result else None

    def get_active_playlists(self):
        """Get all active playlists"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT id, url, name, last_checked, created_date, active
                FROM playlists
                WHERE active = 1
                ORDER BY created_date DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def video_exists(self, video_id: str) -> bool:
        """Check if video already exists in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT 1 FROM videos WHERE video_id = ?',
                (video_id,)
            )
            return cursor.fetchone() is not None

    def add_video(self, video_data):
        """Add a new video to database"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute('''
                    INSERT INTO videos
                    (video_id, title, uploader, duration, upload_date,
                    playlist_id, file_path, metadata, file_hash, file_size, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video_data['video_id'],
                    video_data.get('title', ''),
                    video_data.get('uploader', ''),
                    video_data.get('duration', 0),
                    video_data.get('upload_date', ''),
                    video_data['playlist_id'],
                    video_data.get('file_path', ''),
                    json.dumps(video_data.get('metadata', {})),
                    video_data.get('file_hash', ''),
                    video_data.get('file_size', 0),
                    video_data.get('status', 'downloaded')
                ))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                print(f"Error adding video {video_data.get('video_id', 'unknown')}: {e}")
                return None

    def get_playlist_status_counts(self, playlist_id: int):
        """Get status counts for a playlist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT status, COUNT(*) as count 
                FROM videos 
                WHERE playlist_id = ? 
                GROUP BY status
            ''', (playlist_id,))
            
            results = {row['status']: row['count'] for row in cursor.fetchall()}
            
            return {
                'downloaded': results.get('downloaded', 0),
                'pending': results.get('pending', 0),
                'failed': results.get('failed', 0),
                'duplicate': results.get('duplicate', 0),
                'total': sum(results.values())
            }

    def get_recent_downloads(self, limit: int = 10):
        """Get recent downloads"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT
                    v.title,
                    v.uploader,
                    v.download_date,
                    v.video_id,
                    v.duration,
                    v.file_path,
                    p.name as playlist_name
                FROM videos v
                LEFT JOIN playlists p ON v.playlist_id = p.id
                WHERE v.file_path IS NOT NULL AND v.status = 'downloaded'
                ORDER BY v.download_date DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def deactivate_playlist(self, playlist_id: int):
        """Deactivate a playlist (soft delete)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE playlists SET active = 0 WHERE id = ?',
                (playlist_id,)
            )
            conn.commit()

    def get_file_by_hash(self, file_hash: str):
        """Check if file with same hash exists (duplicate detection)"""
        if not file_hash:
            return None
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                'SELECT * FROM videos WHERE file_hash = ? AND file_hash != ""',
                (file_hash,)
            )
            result = cursor.fetchone()
            return dict(result) if result else None

    def update_playlist_check_time(self, playlist_id: int):
        """Update last checked time for playlist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE playlists SET last_checked = CURRENT_TIMESTAMP WHERE id = ?',
                (playlist_id,)
            )
            conn.commit()

    def log_download_action(self, video_id: str, action: str, details: str = None, playlist_id: int = None, error_message: str = None):
        """Log download actions for debugging"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO download_history
                (video_id, playlist_id, action, details, error_message)
                VALUES (?, ?, ?, ?, ?)
            ''', (video_id, playlist_id, action, details, error_message))
            conn.commit()
