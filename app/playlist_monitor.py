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
        """Perform initial check - Store ytmusicapi data FIRST, then download ALL songs"""
        with self._initial_check_lock:
            if self._is_initial_checking:
                print("⚠️ Another initial check in progress, queuing...")
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
            print(f"Initial check for {len(entries)} videos - downloading ALL songs (no limit)")

            new_downloads = 0
            existing_count = 0
            failed_count = 0

            for i, entry in enumerate(entries):
                if not isinstance(entry, dict) or not entry.get('id'):
                    failed_count += 1
                    continue
                video_id = entry['id']
                print(f"Processing {i+1}/{len(entries)}: {entry.get('title', video_id)}")

                # Check if video is already downloaded or duplicate
                if self.db_manager.video_exists(video_id):
                    existing_count += 1
                    self.db_manager.log_download_action(video_id, 'already_exists', 'Already exists', playlist_id)
                    # Verify that downloaded file still exists; if missing, mark as pending for re-download
                    try:
                        with sqlite3.connect(self.db_manager.db_path) as conn:
                            cursor = conn.execute('SELECT file_path FROM videos WHERE video_id = ? AND status IN ("downloaded", "duplicate")', (video_id,))
                            row = cursor.fetchone()
                            if row and row[0]:
                                if not os.path.exists(row[0]):
                                    print(f"File missing for video {video_id}, marking for re-download")
                                    # Mark as pending
                                    conn.execute('UPDATE videos SET status = "pending" WHERE video_id = ?', (video_id,))
                                    failed_count += 1  # Count as failed for reprocessing
                                    continue
                    except Exception as e:
                        print(f"Error checking file existence for {video_id}: {e}")
                    continue

                try:
                    # STEP 1: Store ytmusicapi metadata in database FIRST (even before download)
                    if not self.db_manager.video_in_database(video_id):
                        # Extract rich metadata from ytmusicapi entry
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
                        
                        # Store in database immediately with pending status and rich metadata
                        video_data = {
                            'video_id': video_id,
                            'title': entry.get('title', 'Unknown Title'),
                            'uploader': entry.get('artist', entry.get('uploader', 'Unknown Artist')),
                            'duration': entry.get('duration', 0),
                            'upload_date': entry.get('upload_date', ''),
                            'playlist_id': playlist_id,
                            'file_path': None,
                            'metadata': ytmusic_metadata,  # Store rich ytmusicapi data
                            'file_hash': None,
                            'status': 'pending',
                            'file_size': 0
                        }
                        self.db_manager.add_video(video_data)
                        print(f"📝 Stored ytmusicapi metadata for: {entry.get('title', video_id)}")

                    # STEP 2: Attempt to download the video
                    download_result = self.downloader.download_video(entry['url'], video_id, playlist_id)
                    
                    if download_result is None or download_result.get('status') == 'failed':
                        failed_count += 1
                        print(f"❌ Download failed for {video_id}, but metadata preserved in database")
                        # Metadata is already safely stored, so we can retry later
                        continue

                    # STEP 3: Check for duplicates by file hash
                    if download_result.get('file_hash'):
                        existing_file = self.db_manager.get_file_by_hash(download_result['file_hash'])
                        if existing_file:
                            print(f"Duplicate detected: {download_result['title']}")
                            # Remove newly downloaded duplicate file
                            if download_result.get('file_path') and os.path.exists(download_result['file_path']):
                                os.remove(download_result['file_path'])
                            download_result['file_path'] = existing_file['file_path']
                            download_result['status'] = 'duplicate'
                            existing_count += 1
                        else:
                            new_downloads += 1
                    else:
                        new_downloads += 1

                    # STEP 4: Update database with download results (preserving existing metadata)
                    # Get existing metadata from database
                    try:
                        with sqlite3.connect(self.db_manager.db_path) as conn:
                            cursor = conn.execute('SELECT metadata FROM videos WHERE video_id = ?', (video_id,))
                            row = cursor.fetchone()
                            if row and row[0]:
                                existing_metadata = json.loads(row[0])
                                # Merge download metadata with existing ytmusicapi metadata
                                combined_metadata = {**existing_metadata, **download_result.get('metadata', {})}
                                download_result['metadata'] = combined_metadata
                    except Exception as e:
                        print(f"Error merging metadata for {video_id}: {e}")

                    # Update the database record with download success
                    download_result['playlist_id'] = playlist_id
                    self._update_video_status(video_id, download_result)
                    
                    self.db_manager.log_download_action(video_id, download_result.get('status'), 'Initial check download', playlist_id)

                    if download_result.get('status') == 'downloaded':
                        print(f"✅ Downloaded: {download_result['title']}")
                        
                except Exception as e:
                    print(f"Error processing {video_id}: {e}")
                    failed_count += 1
                    # Even if download fails, metadata is preserved for retry

            self.db_manager.update_playlist_check_time(playlist_id)
            print(f"✅ Initial check complete: {new_downloads} downloaded, {existing_count} existing, {failed_count} failed/pending")
            print(f"📊 All {len(entries)} songs have ytmusicapi metadata stored in database")
            return new_downloads, existing_count, failed_count
            
        finally:
            with self._initial_check_lock:
                self._is_initial_checking = False


    def check_playlist(self, playlist: dict):
        """Check a single playlist for new videos and download pending ones"""
        try:
            playlist_info = self.downloader.get_playlist_info(playlist['url'])
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

                # Check if video was successfully downloaded or is duplicate
                if self.db_manager.video_exists(video_id):
                    skipped_videos += 1
                    # Also check if downloaded file exists. If missing, set status to pending to retry download
                    try:
                        with sqlite3.connect(self.db_manager.db_path) as conn:
                            cursor = conn.execute('SELECT file_path FROM videos WHERE video_id = ? AND status IN ("downloaded", "duplicate")', (video_id,))
                            row = cursor.fetchone()
                            if row and row[0]:
                                if not os.path.exists(row[0]):
                                    print(f"File missing for video {video_id}, marking for re-download")
                                    conn.execute('UPDATE videos SET status = "pending" WHERE video_id = ?', (video_id,))
                                    skipped_videos -= 1
                    except Exception as e:
                        print(f"Error checking file existence for {video_id}: {e}")
                    continue

                print(f"New video found: {entry['title']}")

                # Download new video
                video_data = self.downloader.download_video(entry['url'], video_id, playlist['id'])
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

                # Add to database or update existing pending entry
                if self.db_manager.video_in_database(video_id):
                    # Update existing pending entry
                    self._update_video_status(video_id, video_data)
                else:
                    # Add new video
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

            # Second, process any pending videos for this playlist
            pending_videos = self.db_manager.get_pending_videos(playlist['id'])
            if pending_videos:
                print(f"📋 Found {len(pending_videos)} pending videos to download")
                for pending in pending_videos:
                    print(f"📥 Processing pending video: {pending['title']}")
                    # Find the video in current playlist info
                    video_url = f"https://www.youtube.com/watch?v={pending['video_id']}"
                    video_data = self.downloader.download_video(video_url, pending['video_id'], playlist['id'])
                    if video_data is None:
                        # Mark as failed
                        self._update_video_status(pending['video_id'], {'status': 'failed'})
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

                    # Update the pending entry
                    self._update_video_status(pending['video_id'], video_data)

                    self.db_manager.log_download_action(
                        pending['video_id'],
                        video_data.get('status', 'processed'),
                        f"Downloaded pending from playlist {playlist['name']}",
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
