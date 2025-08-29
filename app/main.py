from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
from datetime import datetime
import sqlite3
from contextlib import asynccontextmanager

# Import your custom modules
from database import DatabaseManager
from downloader import YouTubeDownloader
from playlist_monitor import PlaylistMonitor
from config import Config

# Fixed database schema migration function
def migrate_database(db_path: str):
    """Migrate database to add missing columns (SQLite compatible)"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check playlists table structure
            cursor.execute("PRAGMA table_info(playlists)")
            columns = [info[1] for info in cursor.fetchall()]
            
            # Add created_date column if missing (SQLite compatible way)
            if 'created_date' not in columns:
                cursor.execute("ALTER TABLE playlists ADD COLUMN created_date TIMESTAMP")
                cursor.execute("UPDATE playlists SET created_date = datetime('now') WHERE created_date IS NULL")
                print("✅ Added 'created_date' column to playlists table")
            
            # Check videos table structure
            cursor.execute("PRAGMA table_info(videos)")
            video_columns = [info[1] for info in cursor.fetchall()]
            
            # Add missing columns to videos table
            if 'file_size' not in video_columns:
                cursor.execute("ALTER TABLE videos ADD COLUMN file_size INTEGER DEFAULT 0")
                cursor.execute("UPDATE videos SET file_size = 0 WHERE file_size IS NULL")
                print("✅ Added 'file_size' column to videos table")
            
            if 'status' not in video_columns:
                cursor.execute("ALTER TABLE videos ADD COLUMN status TEXT DEFAULT 'pending'")
                cursor.execute("UPDATE videos SET status = 'pending' WHERE status IS NULL")
                print("✅ Added 'status' column to videos table")
            
            conn.commit()
            print("✅ Database migration completed successfully")
            
    except Exception as e:
        print(f"❌ Database migration error: {e}")
        return False
    
    return True

# Initialize components
config = Config()

# Run migration before initializing database manager
migrate_database(config.DATABASE_PATH)

# Initialize components after migration
db_manager = DatabaseManager(config.DATABASE_PATH)
downloader = YouTubeDownloader(config.DOWNLOAD_DIR, config.AUDIO_QUALITY)
monitor = PlaylistMonitor(db_manager, downloader)

# Global status tracking
app_status = {
    "monitoring": False,
    "last_check": None,
    "current_activity": "Idle",
    "total_downloads": 0,
    "total_playlists": 0,
    "recent_downloads": []
}

# Modern lifespan event handler (replaces deprecated @app.on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    print("🔄 Starting YouTube Playlist Downloader...")
    
    try:
        playlists = db_manager.get_active_playlists()
        app_status["total_playlists"] = len(playlists)
        
        # Start monitoring
        monitor.start_monitoring()
        app_status["monitoring"] = True
        
        # Add default playlists if any
        for playlist_url in getattr(config, 'DEFAULT_PLAYLISTS', []):
            try:
                # Clean URL (remove HTML entities)
                playlist_url = playlist_url.replace('&amp;', '&')
                existing_playlists = db_manager.get_active_playlists()
                if not any(p['url'] == playlist_url for p in existing_playlists):
                    db_manager.add_playlist(playlist_url, "Default Playlist")
                    print(f"✅ Added default playlist: {playlist_url}")
            except Exception as e:
                print(f"❌ Error adding default playlist: {e}")
        
        print("✅ YouTube Playlist Downloader started successfully!")
        print(f"🌐 Access the web interface at: http://localhost:8080")
        
    except Exception as e:
        print(f"❌ Startup error: {e}")
        app_status["monitoring"] = False
    
    yield  # This is where the app runs
    
    # Shutdown logic
    print("🔄 Shutting down...")
    monitor.stop_monitoring()
    app_status["monitoring"] = False
    print("✅ Shutdown complete")

# Create FastAPI app with lifespan
app = FastAPI(
    title="YouTube Playlist Downloader",
    lifespan=lifespan
)

# Mount static files
try:
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    templates = Jinja2Templates(directory="app/static")
except:
    print("ℹ️ Static files not mounted - running in API mode")
    templates = None

# Pydantic models
class PlaylistRequest(BaseModel):
    url: str
    name: str = None

# Frontend Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request = None):
    """Serve the main frontend page"""
    if templates and request:
        return templates.TemplateResponse("index.html", {"request": request})
    else:
        return HTMLResponse(f"""
        <html><head><title>YouTube Playlist Downloader</title></head>
        <body>
            <h1>🎵 YouTube Playlist Downloader</h1>
            <p>✅ API is running! Web interface available.</p>
            <p>📁 Download directory: <code>{config.DOWNLOAD_DIR}</code></p>
            <p>🔧 Default playlist: <code>{getattr(config, 'DEFAULT_PLAYLISTS', ['None configured'])[0] if hasattr(config, 'DEFAULT_PLAYLISTS') and config.DEFAULT_PLAYLISTS else 'None'}</code></p>
            
            <h2>📡 API Endpoints:</h2>
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
     -d '{{"url":"https://music.youtube.com/playlist?list=YOUR_PLAYLIST_ID","name":"My Music"}}'
            </pre>
            
            <h3>Status Values:</h3>
            <ul>
                <li><strong>downloaded</strong> - Successfully downloaded</li>
                <li><strong>pending</strong> - Queued for download</li>
                <li><strong>failed</strong> - Download failed</li>
                <li><strong>restricted</strong> - Requires authentication</li>
            </ul>
        </body></html>
        """)

# API Routes
@app.get("/api/status")
async def get_status():
    """Get current application status"""
    try:
        playlists = db_manager.get_active_playlists()
        recent = db_manager.get_recent_downloads(10)
        
        return {
            **app_status,
            "total_playlists": len(playlists),
            "check_interval": config.CHECK_INTERVAL,
            "download_dir": config.DOWNLOAD_DIR,
            "recent_downloads": recent
        }
    except Exception as e:
        return {
            "error": str(e),
            "status": "error",
            "monitoring": app_status.get("monitoring", False)
        }

@app.post("/api/playlists")
async def add_playlist(playlist: PlaylistRequest):
    """Add a new playlist to monitor and perform initial check"""
    try:
        # Clean URL (remove HTML entities)
        clean_url = playlist.url.replace('&amp;', '&')
        
        if not ("youtube.com/playlist" in clean_url or "music.youtube.com/playlist" in clean_url):
            raise HTTPException(status_code=400, detail="Invalid YouTube playlist URL")
        
        # Update status
        app_status["current_activity"] = "Adding playlist and checking videos..."
        
        # Extract playlist info
        playlist_info = downloader.get_playlist_info(clean_url)
        if not playlist_info.get('entries'):
            app_status["current_activity"] = "Idle"
            raise HTTPException(status_code=400, detail="Could not extract playlist information or playlist is empty")
        
        # Add to database
        playlist_name = playlist.name or playlist_info.get('title', 'Unnamed Playlist')
        playlist_id = db_manager.add_playlist(clean_url, playlist_name)
        
        if not playlist_id:
            app_status["current_activity"] = "Idle"
            raise HTTPException(status_code=400, detail="Playlist already exists or could not be added")
        
        # Perform initial check and sync
        new_downloads, existing_count, failed_count = monitor.perform_initial_playlist_check(playlist_id, playlist_info)
        
        # Update status
        app_status["total_playlists"] = len(db_manager.get_active_playlists())
        app_status["total_downloads"] += new_downloads
        app_status["current_activity"] = "Idle"
        
        # Get status counts (with safe fallback)
        try:
            status_counts = db_manager.get_playlist_status_counts(playlist_id)
        except:
            status_counts = {'pending': 0, 'downloaded': 0, 'failed': 0}
        
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
    try:
        playlists = db_manager.get_active_playlists()
        
        for playlist in playlists:
            try:
                status_counts = db_manager.get_playlist_status_counts(playlist['id'])
                playlist.update(status_counts)
            except:
                # Fallback if status counts fail
                playlist.update({'downloaded': 0, 'pending': 0, 'failed': 0, 'total': 0})
        
        return playlists
    except Exception as e:
        return {"error": str(e), "playlists": []}

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
    try:
        downloads = db_manager.get_recent_downloads(50)
        return downloads
    except Exception as e:
        return {"error": str(e), "downloads": []}

# Additional utility endpoints
@app.get("/api/stats")
async def get_stats():
    """Get download statistics"""
    try:
        stats = downloader.get_download_stats() if hasattr(downloader, 'get_download_stats') else {}
        return {
            "download_stats": stats,
            "database_stats": {
                "total_videos": len(db_manager.get_recent_downloads(1000)),
                "active_playlists": len(db_manager.get_active_playlists())
            }
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/cleanup")
async def cleanup_downloads():
    """Clean up partial/failed downloads"""
    try:
        if hasattr(downloader, 'cleanup_partial_downloads'):
            downloader.cleanup_partial_downloads()
            return {"success": True, "message": "Cleanup completed"}
        else:
            return {"success": False, "message": "Cleanup not available"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "monitoring": app_status.get("monitoring", False)
    }

if __name__ == "__main__":
    print("🎵 Starting YouTube Playlist Downloader...")
    print("🌐 Access the web interface at: http://localhost:8080")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8080,
        log_level="info"
    )
