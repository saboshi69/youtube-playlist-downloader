import os
import time
import threading
import sqlite3
import json
from typing import Dict
from database import DatabaseManager
from downloader import YouTubeDownloader
from config import Config

class PlaylistMonitor:

    def __init__(self, db_manager: DatabaseManager, downloader: YouTubeDownloader):
        self.db_manager = db_manager
        self.downloader = downloader
        self.config = Config()
        self.running = False
        
        # Concurrency control locks
        self._check_lock = threading.Lock()
        self._is_checking = False
        self._initial_check_lock = threading.Lock()
        self._is_initial_checking = False
        
        # Track what's being processed to prevent duplicates
        self._processing_videos = set()
        self._processing_lock = threading.Lock()

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
        """Perform initial check - Store ytmusicapi data FIRST, then download songs"""
        with self._initial_check_lock:
            if self._is_initial_checking:
                print("⚠️ Another initial check in progress, queuing...")
                time.sleep(5)
                with self._initial_check_lock:
                    if self._is_initial_checking:
                        return 0, 0, 1
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
                print(f"[{i+1}/{len(entries)}] Processing: {entry.get('title', video_id)} ({video_id})")

                # Skip if already being processed
                with self._processing_lock:
                    if video_id in self._processing_videos:
                        print(f"⚠️ Video {video_id} already being processed, skipping")
                        continue
                    self._processing_videos.add(video_id)

                try:
                    # Check if already successfully downloaded
                    if self.db_manager.video_exists(video_id):
                        existing_count += 1
                        print(f"✓ Already exists: {video_id}")
                        continue

                    # STEP 1: Store ytmusicapi metadata in database FIRST
                    if not self.db_manager.video_in_database(video_id):
                        ytmusic_metadata = {
                            'title': entry.get('title', 'Unknown Title'),
                            'artist': entry.get('artist', entry.get('uploader', 'Unknown Artist')),
                            'album': entry.get('album', 'Unknown Album'),
                            'year': entry.get('year'),
                            'thumbnail': entry.get('thumbnail'),
                            'duration_seconds': entry.get('duration', 0),
                            'localization': 'HK_ytmusicapi',
                            'source': 'ytmusicapi_initial'
                        }

                        video_data = {
                            'video_id': video_id,
                            'title': entry.get('title', 'Unknown Title'),
                            'uploader': entry.get('artist', entry.get('uploader', 'Unknown Artist')),
                            'duration': entry.get('duration', 0),
                            'upload_date': entry.get('upload_date', ''),
                            'playlist_id': playlist_id,
                            'file_path': None,
                            'metadata': ytmusic_metadata,
                            'file_hash': None,
                            'status': 'pending',
                            'file_size': 0
                        }

                        self.db_manager.add_video(video_data)
                        print(f"📝 Stored metadata: {entry.get('title', video_id)}")

                    # STEP 2: Attempt download
                    download_result = self.downloader.download_video(entry['url'], video_id, playlist_id)

                    if download_result is None or download_result.get('status') == 'failed':
                        failed_count += 1
                        print(f"❌ Download failed: {video_id}")
                        continue

                    # STEP 3: Check for duplicates and update database
                    if download_result.get('file_hash'):
                        existing_file = self.db_manager.get_file_by_hash(download_result['file_hash'])
                        if existing_file:
                            print(f"🔍 Duplicate hash detected: {download_result['title']}")
                            # Remove the newly downloaded file
                            if download_result.get('file_path') and os.path.exists(download_result['file_path']):
                                try:
                                    os.remove(download_result['file_path'])
                                    print(f"🗑️ Removed duplicate file: {download_result['file_path']}")
                                except Exception as e:
                                    print(f"Error removing duplicate file: {e}")
                            
                            # Point to existing file
                            download_result['file_path'] = existing_file['file_path']
                            download_result['status'] = 'duplicate'
                            existing_count += 1
                        else:
                            new_downloads += 1
                    else:
                        new_downloads += 1

                    # Update database record
                    download_result['playlist_id'] = playlist_id
                    self._update_video_status(video_id, download_result)
                    self.db_manager.log_download_action(video_id, download_result.get('status'), 'Initial check', playlist_id)

                    if download_result.get('status') == 'downloaded':
                        print(f"✅ Successfully downloaded: {download_result['title']}")

                except Exception as e:
                    print(f"❌ Error processing {video_id}: {e}")
                    failed_count += 1
                finally:
                    # Always remove from processing set
                    with self._processing_lock:
                        self._processing_videos.discard(video_id)

            self.db_manager.update_playlist_check_time(playlist_id)
            print(f"✅ Initial check complete: {new_downloads} downloaded, {existing_count} existing, {failed_count} failed")
            return new_downloads, existing_count, failed_count

        finally:
            with self._initial_check_lock:
                self._is_initial_checking = False

    def check_playlist(self, playlist: dict):
        """Check a single playlist for new videos and download pending ones"""
        try:
            # FIXED: Changed from get_playlist_info to get_playlist_info_batch
            playlist_info = self.downloader.get_playlist_info_batch(playlist['url'])
            
            if not playlist_info or not playlist_info.get('entries'):
                print(f"❌ No entries found for playlist: {playlist['url']}")
                return 0

            new_videos = 0
            skipped_videos = 0

            # First, check for new videos from playlist
            for entry in playlist_info['entries']:
                if not isinstance(entry, dict) or not entry.get('id'):
                    continue

                video_id = entry['id']

                # Skip if already being processed
                with self._processing_lock:
                    if video_id in self._processing_videos:
                        print(f"⚠️ Video {video_id} already being processed, skipping")
                        continue

                # Check if already successfully downloaded
                if self.db_manager.video_exists(video_id):
                    skipped_videos += 1
                    continue

                print(f"🆕 New video found: {entry['title']} ({video_id})")

                # Add to processing set
                with self._processing_lock:
                    self._processing_videos.add(video_id)

                try:
                    # Download new video
                    video_data = self.downloader.download_video(entry['url'], video_id, playlist['id'])
                    if video_data is None:
                        continue

                    # Handle duplicates
                    if video_data.get('file_hash'):
                        existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                        if existing_file:
                            print(f"🔍 Duplicate detected: {video_data['title']}")
                            if video_data.get('file_path') and os.path.exists(video_data['file_path']):
                                try:
                                    os.remove(video_data['file_path'])
                                    print(f"🗑️ Removed duplicate file")
                                except Exception as e:
                                    print(f"Error removing duplicate file: {e}")
                            video_data['status'] = 'duplicate'
                            video_data['file_path'] = existing_file['file_path']

                    # Update database
                    if self.db_manager.video_in_database(video_id):
                        self._update_video_status(video_id, video_data)
                    else:
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

                except Exception as e:
                    print(f"❌ Error processing new video {video_id}: {e}")
                finally:
                    # Remove from processing set
                    with self._processing_lock:
                        self._processing_videos.discard(video_id)

            # Second, process any pending videos for this playlist
            pending_videos = self.db_manager.get_pending_videos(playlist['id'])
            if pending_videos:
                print(f"📋 Found {len(pending_videos)} pending videos to download")
                
                for pending in pending_videos:
                    video_id = pending['video_id']
                    
                    # Skip if already being processed
                    with self._processing_lock:
                        if video_id in self._processing_videos:
                            print(f"⚠️ Pending video {video_id} already being processed, skipping")
                            continue
                        self._processing_videos.add(video_id)

                    try:
                        print(f"📥 Processing pending video: {pending['title']} ({video_id})")
                        
                        video_url = f"https://www.youtube.com/watch?v={video_id}"
                        video_data = self.downloader.download_video(video_url, video_id, playlist['id'])
                        
                        if video_data is None:
                            self._update_video_status(video_id, {'status': 'failed'})
                            continue

                        # Handle duplicates
                        if video_data.get('file_hash'):
                            existing_file = self.db_manager.get_file_by_hash(video_data['file_hash'])
                            if existing_file:
                                print(f"🔍 Duplicate detected: {video_data['title']}")
                                if video_data.get('file_path') and os.path.exists(video_data['file_path']):
                                    try:
                                        os.remove(video_data['file_path'])
                                        print(f"🗑️ Removed duplicate file")
                                    except Exception as e:
                                        print(f"Error removing duplicate file: {e}")
                                video_data['status'] = 'duplicate'
                                video_data['file_path'] = existing_file['file_path']

                        # Update the pending entry
                        self._update_video_status(video_id, video_data)
                        self.db_manager.log_download_action(
                            video_id,
                            video_data.get('status', 'processed'),
                            f"Downloaded pending from playlist {playlist['name']}",
                            playlist['id']
                        )

                        if video_data.get('status') == 'downloaded':
                            new_videos += 1

                    except Exception as e:
                        print(f"❌ Error processing pending video {video_id}: {e}")
                    finally:
                        # Remove from processing set
                        with self._processing_lock:
                            self._processing_videos.discard(video_id)

            self.db_manager.update_playlist_check_time(playlist['id'])
            print(f"Playlist check complete: {new_videos} new, {skipped_videos} skipped")
            return new_videos

        except Exception as e:
            print(f"Error checking playlist: {e}")
            return 0

    def _update_video_status(self, video_id: str, video_data: dict):
        """Update existing video record with new data"""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            conn.execute('''
                UPDATE videos
                SET title = ?, uploader = ?, duration = ?, upload_date = ?,
                    file_path = ?, metadata = ?, file_hash = ?, file_size = ?, status = ?
                WHERE video_id = ?
            ''', (
                video_data.get('title', ''),
                video_data.get('uploader', ''),
                video_data.get('duration', 0),
                video_data.get('upload_date', ''),
                video_data.get('file_path', ''),
                json.dumps(video_data.get('metadata', {})),
                video_data.get('file_hash', ''),
                video_data.get('file_size', 0),
                video_data.get('status', 'downloaded'),
                video_id
            ))
            conn.commit()
