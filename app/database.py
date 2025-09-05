import sqlite3
import json
import os
from typing import List, Dict, Optional
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
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'downloaded', 'failed', 'duplicate')),
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

            # Create indexes for better performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_playlist_id ON videos(playlist_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_download_date ON videos(download_date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_video_id ON videos(video_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_file_hash ON videos(file_hash)')

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
        """Check if video already exists and was successfully downloaded"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT 1 FROM videos WHERE video_id = ? AND status IN ("downloaded", "duplicate")',
                (video_id,)
            )
            return cursor.fetchone() is not None

    def video_in_database(self, video_id: str) -> bool:
        """Check if video exists in database regardless of status"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT 1 FROM videos WHERE video_id = ?',
                (video_id,)
            )
            return cursor.fetchone() is not None

    def get_pending_videos(self, playlist_id: int = None):
        """Get all pending videos, optionally filtered by playlist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if playlist_id:
                cursor = conn.execute('''
                    SELECT video_id, title, playlist_id, metadata
                    FROM videos
                    WHERE status = 'pending' AND playlist_id = ?
                    ORDER BY id ASC
                ''', (playlist_id,))
            else:
                cursor = conn.execute('''
                    SELECT video_id, title, playlist_id, metadata
                    FROM videos
                    WHERE status = 'pending'
                    ORDER BY id ASC
                ''')
            return [dict(row) for row in cursor.fetchall()]

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
                    video_data.get('status', 'pending')
                ))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                print(f"Error adding video {video_data.get('video_id', 'unknown')}: {e}")
                return None

    # NEW BATCH INSERT/UPDATE METHODS FOR IMPROVED WORKFLOW

    def upsert_videos_batch(self, videos_data: List[Dict]) -> int:
        """Insert or update videos in batch with conflict resolution"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            sql = '''
                INSERT INTO videos 
                (video_id, title, uploader, duration, upload_date, playlist_id, metadata, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = COALESCE(excluded.title, videos.title),
                    uploader = COALESCE(excluded.uploader, videos.uploader),
                    metadata = excluded.metadata,
                    status = CASE WHEN videos.status IN ('downloaded', 'duplicate', 'processing') 
                             THEN videos.status ELSE excluded.status END
            '''
            
            batch_data = []
            for video in videos_data:
                batch_data.append((
                    video['video_id'],
                    video.get('title', 'Unknown Title'),
                    video.get('uploader', 'Unknown Artist'), 
                    video.get('duration', 0),
                    video.get('upload_date', ''),
                    video['playlist_id'],
                    json.dumps(video.get('metadata', {})),
                    video.get('status', 'pending')
                ))
            
            cursor.executemany(sql, batch_data)
            conn.commit()
            print(f"✅ [UPSERT] Processed {len(batch_data)} videos")
            return cursor.rowcount

    def add_videos_batch(self, videos_data: list) -> int:
        """BATCH INSERT: Add multiple videos in one transaction (ignore duplicates)"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.cursor()
                sql = '''INSERT OR IGNORE INTO videos
                    (video_id, title, uploader, duration, upload_date,
                     playlist_id, file_path, metadata, file_hash, file_size, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                
                batch_data = []
                for video_data in videos_data:
                    row = (
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
                        video_data.get('status', 'pending')
                    )
                    batch_data.append(row)
                
                cursor.executemany(sql, batch_data)
                conn.commit()
                print(f"✅ [BATCH] Inserted {len(batch_data)} videos to database")
                return cursor.rowcount
            except sqlite3.IntegrityError as e:
                print(f"Error in batch insert: {e}")
                return 0

    def get_videos_by_status(self, status: str, playlist_id: int = None):
        """Get videos by status"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if playlist_id:
                cursor = conn.execute(
                    'SELECT * FROM videos WHERE status = ? AND playlist_id = ? ORDER BY id ASC',
                    (status, playlist_id)
                )
            else:
                cursor = conn.execute(
                    'SELECT * FROM videos WHERE status = ? ORDER BY id ASC', 
                    (status,)
                )
            return [dict(row) for row in cursor.fetchall()]

    def update_video_status(self, video_id: str, status: str):
        """Update video status atomically"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE videos SET status = ? WHERE video_id = ?',
                (status, video_id)
            )
            conn.commit()

    def get_video_status(self, video_id: str) -> Optional[str]:
        """Get current status of a video"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT status FROM videos WHERE video_id = ?',
                (video_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def update_video_with_download_result(self, video_id: str, result: Dict):
        """Update video record with download results"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE videos SET 
                    file_path = ?, file_hash = ?, file_size = ?, 
                    status = ?, download_date = CURRENT_TIMESTAMP
                WHERE video_id = ?
            ''', (
                result.get('file_path'),
                result.get('file_hash'),
                result.get('file_size', 0),
                result.get('status', 'downloaded'),
                video_id
            ))
            conn.commit()

    def get_playlist_video_ids(self, playlist_id: int) -> List[str]:
        """Get all video IDs for a playlist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT video_id FROM videos WHERE playlist_id = ?',
                (playlist_id,)
            )
            return [row[0] for row in cursor.fetchall()]

    def update_video_metadata_enriched(self, video_id: str, enriched_metadata: dict):
        """Update video metadata with enriched data"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE videos
                SET metadata = ?
                WHERE video_id = ?
            ''', (json.dumps(enriched_metadata), video_id))
            conn.commit()
            print(f"✅ [ENRICH] Updated enriched metadata for {video_id}")

    # EXISTING METHODS CONTINUE BELOW

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
                'processing': results.get('processing', 0),
                'failed': results.get('failed', 0),
                'duplicate': results.get('duplicate', 0),
                'total': sum(results.values())
            }

    def get_recent_downloads(self, limit: int = 10):
        """Get recent downloads with optimized query"""
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

    # UTILITY METHODS FOR DUAL-SOURCE WORKFLOW

    def compare_video_lists(self, ytmusic_ids: List[str], ytdlp_ids: List[str]) -> Dict[str, List[str]]:
        """Compare two lists of video IDs and return differences"""
        ytmusic_set = set(ytmusic_ids)
        ytdlp_set = set(ytdlp_ids)
        
        return {
            'missing_from_ytmusic': list(ytdlp_set - ytmusic_set),
            'missing_from_ytdlp': list(ytmusic_set - ytdlp_set),
            'common': list(ytmusic_set & ytdlp_set)
        }

    def get_videos_needing_enrichment(self, playlist_id: int) -> List[Dict]:
        """Get videos that need metadata enrichment"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT video_id, title, metadata
                FROM videos
                WHERE playlist_id = ? 
                AND (title = 'Unknown Title' OR uploader = 'Unknown Artist')
                AND status = 'pending'
            ''', (playlist_id,))
            return [dict(row) for row in cursor.fetchall()]

    def mark_videos_as_processing(self, video_ids: List[str]) -> int:
        """Mark multiple videos as processing in batch"""
        if not video_ids:
            return 0
        
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ','.join('?' * len(video_ids))
            cursor = conn.execute(
                f'UPDATE videos SET status = "processing" WHERE video_id IN ({placeholders}) AND status = "pending"',
                video_ids
            )
            conn.commit()
            return cursor.rowcount

    def reset_processing_to_pending(self):
        """Reset any stuck 'processing' videos back to 'pending' (for cleanup on startup)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'UPDATE videos SET status = "pending" WHERE status = "processing"'
            )
            conn.commit()
            if cursor.rowcount > 0:
                print(f"✅ Reset {cursor.rowcount} stuck processing videos to pending")
            return cursor.rowcount
