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
        <html>
        <head><title>YouTube Playlist Downloader</title></head>
        <body>
            <h1>✅ API is running! Web interface available.</h1>
            <p><strong>📁 Download directory:</strong> <code>{config.DOWNLOAD_DIR}</code></p>
            <p><strong>🔧 Default playlist:</strong> <code>{getattr(config, 'DEFAULT_PLAYLISTS', ['None configured'])[0] if hasattr(config, 'DEFAULT_PLAYLISTS') and config.DEFAULT_PLAYLISTS else 'None'}</code></p>
            <h2>API Usage:</h2>
            <pre>
curl -X POST "http://localhost:8080/api/playlists" \\
  -H "Content-Type: application/json" \\
  -d '{{"url":"https://music.youtube.com/playlist?list=YOUR_PLAYLIST_ID","name":"My Music"}}'
            </pre>
        </body>
        </html>
        """)

# API Routes
@app.post("/api/playlists")
async def add_playlist(playlist_request: PlaylistRequest):
    """Add a new playlist to monitor"""
    try:
        playlist_id = db_manager.add_playlist(playlist_request.url, playlist_request.name)
        if playlist_id:
            # Get playlist info using FIXED method name
            try:
                playlist_info = downloader.get_playlist_info_batch(playlist_request.url)
                if playlist_info and playlist_info.get('entries'):
                    # Perform initial check for the playlist
                    new_downloads, existing_count, failed_count = monitor.perform_initial_playlist_check(
                        playlist_id, playlist_info
                    )
                    return {
                        "success": True,
                        "message": f"Playlist added successfully",
                        "playlist_id": playlist_id,
                        "new_downloads": new_downloads,
                        "existing_count": existing_count,
                        "failed_count": failed_count
                    }
                else:
                    return {
                        "success": False,
                        "message": "Could not fetch playlist information",
                        "playlist_id": playlist_id
                    }
            except Exception as e:
                return {
                    "success": False,
                    "message": f"Error fetching playlist: {str(e)}",
                    "playlist_id": playlist_id
                }
        else:
            return {"success": False, "message": "Failed to add playlist or playlist already exists"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error adding playlist: {str(e)}")

@app.get("/api/status")
async def get_status():
    """Get system status"""
    recent_downloads = db_manager.get_recent_downloads(5)
    app_status["recent_downloads"] = recent_downloads
    app_status["total_downloads"] = len(recent_downloads)
    return app_status

@app.post("/api/check")
async def manual_check():
    """Trigger manual check of all playlists"""
    try:
        result = monitor.trigger_manual_check()
        return result
    except Exception as e:
        return {
            "success": False,
            "message": f"Manual check failed: {str(e)}",
            "status": "error"
        }

@app.get("/api/playlists")
async def get_playlists():
    """Get all active playlists with status"""
    try:
        playlists = db_manager.get_active_playlists()
        playlist_status = []
        for playlist in playlists:
            status_counts = db_manager.get_playlist_status_counts(playlist['id'])
            playlist_status.append({
                **playlist,
                **status_counts
            })
        return {"playlists": playlist_status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting playlists: {str(e)}")

@app.delete("/api/playlists/{playlist_id}")
async def deactivate_playlist(playlist_id: int):
    """Deactivate a playlist"""
    try:
        db_manager.deactivate_playlist(playlist_id)
        return {"success": True, "message": "Playlist deactivated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error deactivating playlist: {str(e)}")

@app.get("/api/downloads")
async def get_recent_downloads():
    """Get recent downloads"""
    try:
        downloads = db_manager.get_recent_downloads(20)
        return {"downloads": downloads}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting downloads: {str(e)}")

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
