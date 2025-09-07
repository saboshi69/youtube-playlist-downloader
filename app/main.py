from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
from datetime import datetime
import sqlite3
import os
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
        # Reset any stuck processing videos to pending
        db_manager.reset_processing_to_pending()
        
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
<!DOCTYPE html>
<html>
<head>
    <title>YouTube Playlist Downloader API</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
        .container {{ max-width: 800px; margin: 0 auto; }}
        .header {{ background: #f4f4f4; padding: 20px; border-radius: 8px; }}
        .endpoint {{ background: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎵 YouTube Playlist Downloader API</h1>
            <p>✅ API is running! Web interface available.</p>
        </div>

        <h2>📁 Configuration</h2>
        <p><strong>Download directory:</strong> <code>{config.DOWNLOAD_DIR}</code></p>
        <p><strong>Default playlist:</strong> <code>{getattr(config, 'DEFAULT_PLAYLISTS', ['None configured'])[0] if hasattr(config, 'DEFAULT_PLAYLISTS') and config.DEFAULT_PLAYLISTS else 'None'}</code></p>

        <h2>🚀 Quick Start</h2>
        <div class="endpoint">
            <h3>Add a playlist:</h3>
            <pre><code>curl -X POST "http://localhost:8080/api/playlists" \\
  -H "Content-Type: application/json" \\
  -d '{{"url":"https://music.youtube.com/playlist?list=YOUR_PLAYLIST_ID","name":"My Music"}}'</code></pre>
        </div>

        <div class="endpoint">
            <h3>Check status:</h3>
            <pre><code>curl "http://localhost:8080/api/status"</code></pre>
        </div>

        <div class="endpoint">
            <h3>View downloads:</h3>
            <pre><code>curl "http://localhost:8080/api/downloads"</code></pre>
        </div>

        <h2>📖 API Endpoints</h2>
        <ul>
            <li><code>GET /api/status</code> - Get system status</li>
            <li><code>POST /api/playlists</code> - Add new playlist</li>
            <li><code>GET /api/playlists</code> - List all playlists</li>
            <li><code>DELETE /api/playlists/{{id}}</code> - Remove playlist</li>
            <li><code>POST /api/check</code> - Trigger manual check</li>
            <li><code>GET /api/downloads</code> - Get recent downloads</li>
        </ul>
    </div>
</body>
</html>
        """)

# API Routes
@app.post("/api/playlists")
async def add_playlist(playlist_request: PlaylistRequest, background_tasks: BackgroundTasks):
    """Add a new playlist with dual-source import"""
    try:
        # Add playlist to database
        playlist_id = db_manager.add_playlist(playlist_request.url, playlist_request.name)
        
        if not playlist_id:
            return {"success": False, "message": "Playlist already exists or failed to add"}
        
        # Schedule background dual-source import
        background_tasks.add_task(
            monitor.perform_full_playlist_import,
            playlist_id,
            playlist_request.url
        )
        
        app_status["current_activity"] = f"Importing playlist: {playlist_request.name or 'Untitled'}"
        
        return {
            "success": True,
            "message": "Playlist added and dual-source import started in background",
            "playlist_id": playlist_id,
            "playlist_name": playlist_request.name or "Untitled Playlist"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/status")
async def get_status():
    """Get system status"""
    try:
        recent_downloads = db_manager.get_recent_downloads(5)
        playlists = db_manager.get_active_playlists()
        
        app_status.update({
            "monitoring": monitor.running,
            "total_playlists": len(playlists),
            "total_downloads": len(db_manager.get_recent_downloads(1000)),
            "recent_downloads": recent_downloads,
            "last_check": datetime.now().isoformat() if monitor.running else None
        })
        
        return app_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")

@app.post("/api/check-now")
async def manual_check():
    """Trigger manual check of all playlists"""
    try:
        result = monitor.trigger_manual_check()
        if result.get("success"):
            app_status["current_activity"] = "Manual check in progress..."
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
        
        return playlist_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting playlists: {str(e)}")

@app.get("/api/playlists/{playlist_id}/stats")
async def get_playlist_stats(playlist_id: int):
    """Get status counts for a specific playlist"""
    try:
        stats = db_manager.get_playlist_status_counts(playlist_id)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting playlist stats: {str(e)}")

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
        return downloads
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting downloads: {str(e)}")

@app.post("/api/validate-downloads")
async def validate_downloads():
    """Validate that downloaded files actually exist in the folder and fix database"""
    try:
        # Get all videos marked as 'downloaded'
        downloaded_videos = db_manager.get_videos_by_status('downloaded')
        
        valid_count = 0
        fixed_count = 0
        
        for video in downloaded_videos:
            file_path = video.get('file_path')
            video_id = video.get('video_id')
            
            # Check if file actually exists
            if file_path and os.path.exists(file_path):
                valid_count += 1
            else:
                # File missing - mark as pending for re-download
                print(f"🔍 [VALIDATE] Missing file for {video_id}: {file_path}")
                db_manager.update_video_status(video_id, 'pending')
                fixed_count += 1
        
        return {
            "success": True,
            "valid_count": valid_count,
            "fixed_count": fixed_count,
            "message": f"Validation complete. {valid_count} files valid, {fixed_count} marked for re-download."
        }
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        return {
            "success": False,
            "message": str(e)
        }

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
