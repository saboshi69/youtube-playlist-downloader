import os
import time
import threading
from typing import Dict
import yt_dlp

from database import DatabaseManager
from downloader import YouTubeDownloader
from config import Config

class PlaylistMonitor:
    def __init__(self, db_manager: DatabaseManager, downloader: YouTubeDownloader):
        self.db_manager = db_manager
        self.downloader = downloader
        self.config = Config()
        self.running = False
        
        # ADD: Concurrency control
        self._check_lock = threading.Lock()
        self._is_checking = False
        self._initial_check_lock = threading.Lock()
        self._is_initial_checking = False

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
        """Main monitoring loop with concurrency control"""
        while self.running:
            try:
                # Only run if not already checking
                with self._check_lock:
                    if self._is_checking:
                        print("⚠️ Scheduled check skipped - manual check in progress")
                        time.sleep(self.config.CHECK_INTERVAL)
                        continue
                    self._is_checking = True

                try:
                    total_new = self.check_all_playlists()
                    print(f"Scheduled monitor check completed: {total_new} new videos")
                finally:
                    with self._check_lock:
                        self._is_checking = False

                time.sleep(self.config.CHECK_INTERVAL)
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                with self._check_lock:
                    self._is_checking = False
                time.sleep(60)

    def trigger_manual_check(self):
        """Trigger manual check with concurrency protection"""
        with self._check_lock:
            if self._is_checking:
                return {
                    "success": False, 
                    "message": "Check already in progress. Please wait...",
                    "status": "already_running"
                }
            self._is_checking = True

        try:
            print("🔄 Manual check triggered")
            total_new = self.check_all_playlists()
            return {
                "success": True,
                "message": f"Manual check completed. Found {total_new} new songs.",
                "new_songs": total_new,
                "status": "completed"
            }
        except Exception as e:
            print(f"Manual check failed: {e}")
            return {
                "success": False,
                "message": f"Check failed: {str(e)}",
                "status": "failed"
            }
        finally:
            with self._check_lock:
                self._is_checking = False

    def check_all_playlists(self):
        """Check all active playlists for new videos"""
        playlists = self.db_manager.get_active_playlists()
        total_new = 0
        
        for playlist in playlists:
            print(f"Checking playlist: {playlist['name'] or playlist['url']}")
            new_count = self.check_playlist(playlist)
            total_new += new_count
            
        return total_new

    def perform_initial_playlist_check(self, playlist_id: int, playlist_info: dict):
        """Perform initial check with concurrency protection"""
        with self._initial_check_lock:
            if self._is_initial_checking:
                print("⚠️ Another initial check in progress, queuing...")
                # Wait briefly and try again
                time.sleep(5)
                with self._initial_check_lock:
                    if self._is_initial_checking:
                        return 0, 0, 1  # Skip if still busy
            self._is_initial_checking = True

        try:
            print(f"🔄 Starting initial check for playlist {playlist_id}")
            
            if not playlist_info or not isinstance(playlist_info, dict):
                return 0, 0, 1
            
            entries = playlist_info.get('entries', [])
            print(f"Initial check for {len(entries)} videos")
            
            new_downloads = 0
            existing_count = 0
            failed_count = 0
            
            for i, entry in enumerate(entries):
                if not isinstance(entry, dict) or not entry.get('id'):
                    failed_count += 1
                    continue
                
                video_id = entry['id']
                print(f"Processing {i+1}/{len(entries)}: {entry.get('title', video_id)}")

                if self.db_manager.video_exists(video_id):
                    existing_count += 1
                    self.db_manager.log_download_action(video_id, 'already_exists', 'Already exists', playlist_id)
                    continue

                try:
                    # Limit initial downloads to prevent overwhelming
                    if new_downloads >= 3:
                        # Add remaining as pending
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

                    # Download video (includes automatic wait if successful)
                    video_data = self.downloader.download_video(entry['url'], video_id)
                    
                    if video_data is None or video_data.get('status') == 'failed':
                        failed_count += 1
                        # Add as failed and continue
                        continue

                    # Check for duplicates by hash
                    if video_data.get('file_hash'):
                        existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                        if existing_file:
                            print(f"Duplicate detected: {video_data['title']}")
                            if video_data.get('file_path') and os.path.exists(video_data['file_path']):
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
                    self.db_manager.log_download_action(video_id, video_data.get('status'), 'Initial check', playlist_id)

                    if video_data.get('status') == 'downloaded':
                        print(f"✅ Downloaded: {video_data['title']}")

                except Exception as e:
                    print(f"Error processing {video_id}: {e}")
                    failed_count += 1

            self.db_manager.update_playlist_check_time(playlist_id)
            print(f"✅ Initial check complete: {new_downloads} downloaded, {existing_count} existing, {failed_count} failed")
            return new_downloads, existing_count, failed_count
            
        finally:
            with self._initial_check_lock:
                self._is_initial_checking = False

    def check_playlist(self, playlist: dict):
        """Check a single playlist for new videos"""
        try:
            playlist_info = self.downloader.get_playlist_info(playlist['url'])
            
            if not playlist_info or not playlist_info.get('entries'):
                print(f"❌ No entries found for playlist: {playlist['url']}")
                return 0
            
            new_videos = 0
            skipped_videos = 0
            
            for entry in playlist_info['entries']:
                if not isinstance(entry, dict) or not entry.get('id'):
                    continue
                
                video_id = entry['id']
                
                if self.db_manager.video_exists(video_id):
                    skipped_videos += 1
                    continue

                print(f"New video found: {entry['title']}")
                
                video_data = self.downloader.download_video(entry['url'], video_id)
                
                if video_data is None:
                    continue

                # Check for duplicates by hash
                if video_data.get('file_hash'):
                    existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                    if existing_file:
                        print(f"Duplicate detected: {video_data['title']}")
                        if video_data.get('file_path') and os.path.exists(video_data['file_path']):
                            os.remove(video_data['file_path'])
                        video_data['status'] = 'duplicate'
                        video_data['file_path'] = existing_file['file_path']

                # Add to database
                video_data['playlist_id'] = playlist['id']
                self.db_manager.add_video(video_data)
                self.db_manager.log_download_action(
                    video_id,
                    video_data.get('status', 'processed'),
                    f"Downloaded from playlist {playlist['name']}",
                    playlist['id']
                )

                if video_data.get('status') == 'downloaded':
                    new_videos += 1

            self.db_manager.update_playlist_check_time(playlist['id'])
            print(f"Playlist check complete: {new_videos} new, {skipped_videos} skipped")
            return new_videos

        except Exception as e:
            print(f"Error checking playlist: {e}")
            return 0
