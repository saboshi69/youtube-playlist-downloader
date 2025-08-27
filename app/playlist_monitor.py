import time
import threading
from typing import List
from database import DatabaseManager
from downloader import YouTubeDownloader
from config import Config

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
    
    def check_all_playlists(self):
        """Check all active playlists for new videos"""
        playlists = self.db_manager.get_active_playlists()
        
        for playlist in playlists:
            print(f"Checking playlist: {playlist['name'] or playlist['url']}")
            self.check_playlist(playlist)
    
    def check_playlist(self, playlist: dict):
        """Check a single playlist for new videos"""
        try:
            playlist_info = self.downloader.get_playlist_info(playlist['url'])
            new_videos = 0
            skipped_videos = 0
            
            for entry in playlist_info['entries']:
                video_id = entry['id']
                
                # Check if video already exists
                if self.db_manager.video_exists(video_id):
                    skipped_videos += 1
                    continue
                
                print(f"New video found: {entry['title']}")
                
                # Check for duplicate by hash (if file already downloaded)
                video_data = self.downloader.download_video(entry['url'], video_id)
                if video_data is None:
                    continue
                
                # Check if file with same hash already exists
                if video_data['file_hash']:
                    existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                    if existing_file:
                        print(f"Duplicate file detected: {video_data['title']} (matches {existing_file['title']})")
                        # Remove the newly downloaded file
                        if os.path.exists(video_data['file_path']):
                            os.remove(video_data['file_path'])
                        continue
                
                # Add to database
                video_data['playlist_id'] = playlist['id']
                self.db_manager.add_video(video_data)
                self.db_manager.log_download_action(
                    video_id, 
                    'downloaded', 
                    f"Downloaded from playlist {playlist['name']}"
                )
                
                new_videos += 1
                print(f"Successfully downloaded: {video_data['title']}")
            
            # Update playlist check time
            self.db_manager.update_playlist_check_time(playlist['id'])
            
            print(f"Playlist check complete: {new_videos} new, {skipped_videos} skipped")
            
        except Exception as e:
            print(f"Error checking playlist {playlist['url']}: {e}")
            self.db_manager.log_download_action(
                'playlist_error', 
                'error', 
                f"Error checking playlist: {str(e)}"
            )
