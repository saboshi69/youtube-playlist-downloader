from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
from typing import List, Optional, Dict
import os
import json
import asyncio
from datetime import datetime

from config import Config
from database import DatabaseManager
from downloader import YouTubeDownloader
from playlist_monitor import PlaylistMonitor

app = FastAPI(title="YouTube Playlist Downloader")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/static")

# Initialize components
config = Config()
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

class PlaylistRequest(BaseModel):
    url: str
    name: Optional[str] = None

@app.on_event("startup")
async def startup_event():
    """Start monitoring when app starts"""
    # Load existing playlists count
    playlists = db_manager.get_active_playlists()
    app_status["total_playlists"] = len(playlists)
    
    # Start monitoring
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
    return templates.TemplateResponse("index.html", {"request": request})

# API Routes for Frontend
@app.get("/api/status")
async def get_status():
    """Get current application status"""
    playlists = db_manager.get_active_playlists()
    
    # Get recent downloads (last 10)
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
    """Add a new playlist to monitor"""
    try:
        # Validate URL first
        if not ("youtube.com/playlist" in playlist.url or "music.youtube.com/playlist" in playlist.url):
            raise HTTPException(status_code=400, detail="Invalid YouTube playlist URL")
        
        # Extract playlist info
        playlist_info = downloader.get_playlist_info(playlist.url)
        if not playlist_info.get('entries'):
            raise HTTPException(status_code=400, detail="Could not extract playlist information or playlist is empty")
        
        # Add to database
        playlist_name = playlist.name or playlist_info.get('title', 'Unnamed Playlist')
        playlist_id = db_manager.add_playlist(playlist.url, playlist_name)
        
        # Update status
        app_status["total_playlists"] += 1
        
        return {
            "success": True,
            "message": f"Added playlist: {playlist_name}",
            "id": playlist_id,
            "video_count": len(playlist_info['entries'])
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/playlists")
async def get_playlists():
    """Get all monitored playlists with status"""
    playlists = db_manager.get_active_playlists()
    
    # Add video counts for each playlist
    for playlist in playlists:
        playlist['video_count'] = db_manager.get_playlist_video_count(playlist['id'])
        playlist['downloaded_count'] = db_manager.get_playlist_downloaded_count(playlist['id'])
    
    return playlists

@app.delete("/api/playlists/{playlist_id}")
async def remove_playlist(playlist_id: int):
    """Remove a playlist from monitoring"""
    try:
        db_manager.deactivate_playlist(playlist_id)
        app_status["total_playlists"] = max(0, app_status["total_playlists"] - 1)
        return {"success": True, "message": "Playlist removed from monitoring"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/check-now")
async def trigger_check():
    """Manually trigger playlist checking"""
    try:
        app_status["current_activity"] = "Checking playlists..."
        app_status["last_check"] = datetime.now().isoformat()
        
        # Run check in background
        asyncio.create_task(run_playlist_check())
        
        return {"success": True, "message": "Playlist check started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def run_playlist_check():
    """Background task to check playlists"""
    try:
        new_downloads = monitor.check_all_playlists()
        app_status["current_activity"] = "Idle"
        app_status["total_downloads"] += new_downloads
        
        # Update recent downloads
        recent = db_manager.get_recent_downloads(5)
        app_status["recent_downloads"] = recent
        
    except Exception as e:
        app_status["current_activity"] = f"Error: {str(e)}"
        print(f"Error in background check: {e}")

@app.get("/api/downloads")
async def get_downloads():
    """Get recent downloads"""
    downloads = db_manager.get_recent_downloads(50)
    return downloads

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs(os.path.dirname(config.DATABASE_PATH), exist_ok=True)
    os.makedirs("app/static", exist_ok=True)
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8080,
        log_level="info"
    )
