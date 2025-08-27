# Add these methods to the existing DatabaseManager class

def get_playlist_video_count(self, playlist_id: int) -> int:
    """Get total video count for a playlist"""
    with sqlite3.connect(self.db_path) as conn:
        cursor = conn.execute(
            'SELECT COUNT(*) FROM videos WHERE playlist_id = ?',
            (playlist_id,)
        )
        return cursor.fetchone()[0]

def get_playlist_downloaded_count(self, playlist_id: int) -> int:
    """Get downloaded video count for a playlist"""
    with sqlite3.connect(self.db_path) as conn:
        cursor = conn.execute(
            'SELECT COUNT(*) FROM videos WHERE playlist_id = ? AND file_path IS NOT NULL',
            (playlist_id,)
        )
        return cursor.fetchone()[0]

def get_recent_downloads(self, limit: int = 10) -> List[Dict]:
    """Get recent downloads"""
    with sqlite3.connect(self.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('''
            SELECT v.title, v.uploader, v.download_date, p.name as playlist_name
            FROM videos v
            JOIN playlists p ON v.playlist_id = p.id
            WHERE v.file_path IS NOT NULL
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
