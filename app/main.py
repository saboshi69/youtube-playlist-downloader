from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
import os
import json
import asyncio
from datetime import datetime
import sqlite3
import threading
import time
import yt_dlp
import hashlib
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC

app = FastAPI(title="YouTube Playlist Downloader")

# Configuration
class Config:
    DATABASE_PATH = os.getenv('DATABASE_PATH', './data/downloads.db')
    DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', './downloads/music')
    CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 3600))  # seconds
    DOWNLOAD_FORMAT = os.getenv('DOWNLOAD_FORMAT', 'mp3')
    AUDIO_QUALITY = os.getenv('AUDIO_QUALITY', '320')

# Database Manager
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
                    status TEXT DEFAULT 'pending',
                    file_size INTEGER DEFAULT 0,
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
    
    def add_playlist(self, url: str, name: str = None) -> int:
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
                # Playlist already exists, return existing ID
                cursor = conn.execute('SELECT id FROM playlists WHERE url = ?', (url,))
                result = cursor.fetchone()
                return result[0] if result else None
    
    def get_active_playlists(self):
        """Get all active playlists"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                'SELECT * FROM playlists WHERE active = 1 ORDER BY created_date DESC'
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def video_exists(self, video_id: str) -> bool:
        """Check if video already exists in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT 1 FROM videos WHERE video_id = ?',
                (video_id,)
            )
            return cursor.fetchone() is not None
    
    def add_video(self, video_data) -> int:
        """Add a new video to database"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute('''
                    INSERT INTO videos 
                    (video_id, title, uploader, duration, upload_date, 
                     playlist_id, file_path, metadata, file_hash, status, file_size)
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
                    video_data.get('status', 'pending'),
                    video_data.get('file_size', 0)
                ))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                print(f"Video already exists: {video_data.get('video_id', 'unknown')}")
                return None
    
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
                'SELECT COUNT(*) FROM videos WHERE playlist_id = ? AND status = "downloaded" AND file_path IS NOT NULL',
                (playlist_id,)
            )
            return cursor.fetchone()[0]
    
    def get_playlist_status_counts(self, playlist_id: int):
        """Get status counts for a playlist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT 
                    status,
                    COUNT(*) as count
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
                SELECT v.title, v.uploader, v.download_date, v.video_id, p.name as playlist_name
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

# YouTube Downloader
class YouTubeDownloader:
    def __init__(self, download_dir: str, audio_quality: str = '320'):
        self.download_dir = download_dir
        self.audio_quality = audio_quality
        os.makedirs(download_dir, exist_ok=True)
    
    def get_playlist_info(self, playlist_url: str):
        """Extract playlist information without downloading"""
        ydl_opts = {
            'extract_flat': True,
            'quiet': True,
            'no_warnings': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(playlist_url, download=False)
                return {
                    'title': info.get('title'),
                    'entries': [
                        {
                            'id': entry.get('id'),
                            'title': entry.get('title'),
                            'url': f"https://youtube.com/watch?v={entry.get('id')}"
                        }
                        for entry in info.get('entries', [])
                        if entry and entry.get('id')
                    ]
                }
            except Exception as e:
                print(f"Error extracting playlist info: {e}")
                return {'title': None, 'entries': []}
    
    def download_video(self, video_url: str, video_id: str):
        """Download a single video and return metadata"""
        safe_title = f"video_{video_id}"
        file_path = os.path.join(self.download_dir, f"{safe_title}.mp3")
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': self.audio_quality,
            'embed_metadata': True,
            'writeinfojson': False,
            'ignoreerrors': True,
            'no_warnings': True,
            'quiet': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info first
                info = ydl.extract_info(video_url, download=False)
                
                if not info:
                    return None
                
                # Update filename with actual title
                safe_title = self._sanitize_filename(info.get('title', f'video_{video_id}'))
                file_path = os.path.join(self.download_dir, f"{safe_title}.mp3")
                ydl_opts['outtmpl'] = file_path.replace('.mp3', '.%(ext)s')
                
                # Download
                ydl.download([video_url])
                
                # Calculate file hash and size
                file_hash = self._calculate_file_hash(file_path) if os.path.exists(file_path) else None
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                
                return {
                    'video_id': video_id,
                    'title': info.get('title', ''),
                    'uploader': info.get('uploader', ''),
                    'duration': info.get('duration', 0),
                    'upload_date': info.get('upload_date', ''),
                    'file_path': file_path if os.path.exists(file_path) else None,
                    'file_hash': file_hash,
                    'file_size': file_size,
                    'status': 'downloaded' if os.path.exists(file_path) else 'failed',
                    'metadata': {
                        'description': info.get('description', ''),
                        'view_count': info.get('view_count', 0),
                    }
                }
        except Exception as e:
            print(f"Error downloading {video_id}: {e}")
            return {
                'video_id': video_id,
                'title': f'Failed: {video_id}',
                'uploader': '',
                'duration': 0,
                'upload_date': '',
                'file_path': None,
                'file_hash': None,
                'file_size': 0,
                'status': 'failed',
                'metadata': {}
            }
    
    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:200]  # Limit length
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file for duplicate detection"""
        if not os.path.exists(file_path):
            return None
        
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

# Playlist Monitor
class PlaylistMonitor:
    def __init__(self, db_manager: DatabaseManager, downloader: YouTubeDownloader):
        self.db_manager = db_manager
        self.downloader = downloader
        self.config = Config()
        self.running = False
    
    def start_monitoring(self):
        """Start the monitoring loop"""
        self.running = True
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        print(f"Started playlist monitoring with {self.config.CHECK_INTERVAL}s interval")
    
    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.running = False
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self.check_all_playlists()
                time.sleep(self.config.CHECK_INTERVAL)
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def check_all_playlists(self) -> int:
        """Check all active playlists for new videos"""
        playlists = self.db_manager.get_active_playlists()
        total_new = 0
        
        for playlist in playlists:
            print(f"Checking playlist: {playlist['name'] or playlist['url']}")
            new_count = self.check_playlist(playlist)
            total_new += new_count
        
        return total_new
    
    def check_playlist(self, playlist: dict) -> int:
        """Check a single playlist for new videos"""
        try:
            playlist_info = self.downloader.get_playlist_info(playlist['url'])
            new_videos = 0
            
            for entry in playlist_info['entries'][:10]:  # Check first 10 videos
                video_id = entry['id']
                
                # Check if video already exists
                if self.db_manager.video_exists(video_id):
                    continue
                
                print(f"New video found: {entry['title']}")
                
                # Download video
                video_data = self.downloader.download_video(entry['url'], video_id)
                if video_data is None:
                    continue
                
                # Check for duplicates by hash
                if video_data.get('file_hash'):
                    existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                    if existing_file:
                        print(f"Duplicate detected: {video_data['title']}")
                        # Remove the newly downloaded file
                        if video_data['file_path'] and os.path.exists(video_data['file_path']):
                            os.remove(video_data['file_path'])
                        video_data['status'] = 'duplicate'
                        video_data['file_path'] = existing_file['file_path']
                
                # Add to database
                video_data['playlist_id'] = playlist['id']
                self.db_manager.add_video(video_data)
                self.db_manager.log_download_action(
                    video_id, 
                    video_data['status'], 
                    f"Processed during monitoring check", 
                    playlist['id']
                )
                
                if video_data['status'] == 'downloaded':
                    new_videos += 1
                    print(f"Successfully downloaded: {video_data['title']}")
            
            # Update playlist check time
            self.db_manager.update_playlist_check_time(playlist['id'])
            
            print(f"Playlist check complete: {new_videos} new videos")
            return new_videos
            
        except Exception as e:
            print(f"Error checking playlist {playlist['url']}: {e}")
            return 0
    
    def perform_initial_playlist_check(self, playlist_id: int, playlist_info: dict):
        """Perform initial check when adding a new playlist"""
        new_downloads = 0
        existing_count = 0
        failed_count = 0
        
        print(f"Starting initial check for playlist with {len(playlist_info['entries'])} videos")
        
        for i, entry in enumerate(playlist_info['entries']):
            video_id = entry['id']
            
            print(f"Processing video {i+1}/{len(playlist_info['entries'])}: {entry.get('title', video_id)}")
            
            # Check if video already exists in database
            if self.db_manager.video_exists(video_id):
                existing_count += 1
                self.db_manager.log_download_action(video_id, 'already_exists', f"Video already in database", playlist_id)
                continue
            
            try:
                # Limit initial downloads to prevent overwhelming
                if new_downloads >= 3:  # Only download first 3 videos immediately
                    # Add to database as pending
                    video_data = {
                        'video_id': video_id,
                        'title': entry.get('title', 'Unknown'),
                        'uploader': 'Unknown',
                        'duration': 0,
                        'upload_date': '',
                        'playlist_id': playlist_id,
                        'file_path': None,
                        'metadata': {},
                        'file_hash': None,
                        'status': 'pending',
                        'file_size': 0
                    }
                    self.db_manager.add_video(video_data)
                    continue
                
                # Download the video
                video_data = self.downloader.download_video(entry['url'], video_id)
                
                if video_data is None or video_data.get('status') == 'failed':
                    failed_count += 1
                    if video_data is None:
                        video_data = {
                            'video_id': video_id,
                            'title': entry.get('title', 'Unknown'),
                            'uploader': 'Unknown',
                            'duration': 0,
                            'upload_date': '',
                            'playlist_id': playlist_id,
                            'file_path': None,
                            'metadata': {},
                            'file_hash': None,
                            'status': 'failed',
                            'file_size': 0
                        }
                    video_data['playlist_id'] = playlist_id
                    self.db_manager.add_video(video_data)
                    self.db_manager.log_download_action(video_id, 'failed', f"Download failed during initial check", playlist_id)
                    continue
                
                # Check for duplicates by hash
                if video_data.get('file_hash'):
                    existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                    if existing_file:
                        print(f"Duplicate detected: {video_data['title']} (matches {existing_file['title']})")
                        # Remove the newly downloaded file
                        if video_data['file_path'] and os.path.exists(video_data['file_path']):
                            os.remove(video_data['file_path'])
                        
                        video_data['file_path'] = existing_file['file_path']
                        video_data['status'] = 'duplicate'
                        existing_count += 1
                    else:
                        new_downloads += 1
                else:
                    new_downloads += 1
                
                # Add to database
                video_data['playlist_id'] = playlist_id
                self.db_manager.add_video(video_data)
                self.db_manager.log_download_action(video_id, video_data['status'], f"Processed during initial playlist check", playlist_id)
                
                if video_data['status'] == 'downloaded':
                    print(f"Successfully downloaded: {video_data['title']}")
                
            except Exception as e:
                print(f"Error processing video {video_id}: {e}")
                failed_count += 1
                # Add as failed
                video_data = {
                    'video_id': video_id,
                    'title': entry.get('title', 'Unknown'),
                    'uploader': 'Unknown',
                    'duration': 0,
                    'upload_date': '',
                    'playlist_id': playlist_id,
                    'file_path': None,
                    'metadata': {},
                    'file_hash': None,
                    'status': 'failed',
                    'file_size': 0
                }
                self.db_manager.add_video(video_data)
                self.db_manager.log_download_action(video_id, 'error', f"Error: {str(e)}", playlist_id, str(e))
        
        # Update playlist check time
        self.db_manager.update_playlist_check_time(playlist_id)
        
        return new_downloads, existing_count, failed_count

# Initialize components
config = Config()
db_manager = DatabaseManager(config.DATABASE_PATH)
downloader = YouTubeDownloader(config.DOWNLOAD_DIR, config.AUDIO_QUALITY)
monitor = PlaylistMonitor(db_manager, downloader)

# Mount static files
try:
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    templates = Jinja2Templates(directory="app/static")
except:
    print("Static files not mounted - running in simple mode")
    templates = None

# Global status tracking
app_status = {
    "monitoring": False,
    "last_check": None,
    "current_activity": "Idle",
    "total_downloads": 0,
    "total_playlists": 0,
    "recent_downloads": []
}

class PlaylistRequest(BaseModel):
    url: str
    name: str = None

@app.on_event("startup")
async def startup_event():
    """Start monitoring when app starts"""
    playlists = db_manager.get_active_playlists()
    app_status["total_playlists"] = len(playlists)
    
    monitor.start_monitoring()
    app_status["monitoring"] = True
    print("YouTube Playlist Downloader started successfully!")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop monitoring when app shuts down"""
    monitor.stop_monitoring()
    app_status["monitoring"] = False

# Frontend Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main frontend page"""
    if templates:
        return templates.TemplateResponse("index.html", {"request": request})
    else:
        return HTMLResponse("""
        <html><head><title>YouTube Playlist Downloader</title></head>
        <body>
            <h1>🎵 YouTube Playlist Downloader</h1>
            <p>✅ API is running! Web interface available.</p>
            <h2>Available API Endpoints:</h2>
            <ul>
                <li><strong>GET /api/status</strong> - Check application status</li>
                <li><strong>POST /api/playlists</strong> - Add new playlist</li>
                <li><strong>GET /api/playlists</strong> - List all playlists</li>
                <li><strong>POST /api/check-now</strong> - Trigger immediate check</li>
                <li><strong>GET /api/downloads</strong> - Get recent downloads</li>
            </ul>
            <h3>Example: Add a playlist</h3>
            <pre>
curl -X POST "http://localhost:8080/api/playlists" \\
     -H "Content-Type: application/json" \\
     -d '{"url":"https://www.youtube.com/playlist?list=YOUR_PLAYLIST_ID","name":"My Music"}'
            </pre>
        </body></html>
        """)

# API Routes
@app.get("/api/status")
async def get_status():
    """Get current application status"""
    playlists = db_manager.get_active_playlists()
    recent = db_manager.get_recent_downloads(10)
    
    return {
        **app_status,
        "total_playlists": len(playlists),
        "check_interval": config.CHECK_INTERVAL,
        "download_dir": config.DOWNLOAD_DIR,
        "recent_downloads": recent
    }

@app.post("/api/playlists")
async def add_playlist(playlist: PlaylistRequest):
    """Add a new playlist to monitor and perform initial check"""
    try:
        if not ("youtube.com/playlist" in playlist.url or "music.youtube.com/playlist" in playlist.url):
            raise HTTPException(status_code=400, detail="Invalid YouTube playlist URL")
        
        # Update status
        app_status["current_activity"] = "Adding playlist and checking videos..."
        
        # Extract playlist info
        playlist_info = downloader.get_playlist_info(playlist.url)
        if not playlist_info.get('entries'):
            app_status["current_activity"] = "Idle"
            raise HTTPException(status_code=400, detail="Could not extract playlist information or playlist is empty")
        
        # Add to database
        playlist_name = playlist.name or playlist_info.get('title', 'Unnamed Playlist')
        playlist_id = db_manager.add_playlist(playlist.url, playlist_name)
        
        if not playlist_id:
            app_status["current_activity"] = "Idle"
            raise HTTPException(status_code=400, detail="Playlist already exists or could not be added")
        
        # Perform initial check and sync
        new_downloads, existing_count, failed_count = monitor.perform_initial_playlist_check(playlist_id, playlist_info)
        
        # Update status
        app_status["total_playlists"] = len(db_manager.get_active_playlists())
        app_status["total_downloads"] += new_downloads
        app_status["current_activity"] = "Idle"
        
        # Get status counts
        status_counts = db_manager.get_playlist_status_counts(playlist_id)
        
        return {
            "success": True,
            "message": f"Added playlist: {playlist_name}",
            "id": playlist_id,
            "total_videos": len(playlist_info['entries']),
            "new_downloads": new_downloads,
            "existing_videos": existing_count,
            "failed_videos": failed_count,
            "pending_videos": status_counts.get('pending', 0),
            "details": f"Found {len(playlist_info['entries'])} videos. Downloaded {new_downloads} new, {existing_count} already existed, {failed_count} failed, {status_counts.get('pending', 0)} pending."
        }
    
    except HTTPException:
        raise
    except Exception as e:
        app_status["current_activity"] = "Idle"
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/playlists")
async def get_playlists():
    """Get all monitored playlists with status"""
    playlists = db_manager.get_active_playlists()
    
    for playlist in playlists:
        status_counts = db_manager.get_playlist_status_counts(playlist['id'])
        playlist.update(status_counts)
    
    return playlists

@app.delete("/api/playlists/{playlist_id}")
async def remove_playlist(playlist_id: int):
    """Remove a playlist from monitoring"""
    try:
        db_manager.deactivate_playlist(playlist_id)
        app_status["total_playlists"] = len(db_manager.get_active_playlists())
        return {"success": True, "message": "Playlist removed from monitoring"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/check-now")
async def trigger_check():
    """Manually trigger playlist checking"""
    try:
        app_status["current_activity"] = "Checking playlists..."
        app_status["last_check"] = datetime.now().isoformat()
        
        new_downloads = monitor.check_all_playlists()
        app_status["current_activity"] = "Idle"
        app_status["total_downloads"] += new_downloads
        
        return {"success": True, "message": f"Playlist check completed. Found {new_downloads} new songs."}
    except Exception as e:
        app_status["current_activity"] = "Idle"
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/downloads")
async def get_downloads():
    """Get recent downloads"""
    downloads = db_manager.get_recent_downloads(50)
    return downloads

if __name__ == "__main__":
    print("Starting YouTube Playlist Downloader...")
    print("Access the web interface at: http://localhost:8080")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8080,
        log_level="info"
    )
