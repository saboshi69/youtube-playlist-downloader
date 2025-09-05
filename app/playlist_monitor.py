import os
import time
import threading
import sqlite3
import json
from typing import Dict, List
from database import DatabaseManager
from downloader import YouTubeDownloader
from config import Config

class PlaylistMonitor:
    def __init__(self, db_manager: DatabaseManager, downloader: YouTubeDownloader):
        self.db_manager = db_manager
        self.downloader = downloader
        self.config = Config()
        self.running = False

        # FIXED: Single master lock for ALL operations
        self._master_lock = threading.RLock()  # Reentrant lock
        self._is_checking = False              # Interval monitor running
        self._is_initial_checking = False      # Initial import running
        self._is_downloading = False           # Download process running
        
        # Track what's being processed to prevent duplicates
        self._processing_videos = set()
        self._processing_lock = threading.Lock()

    def start_monitoring(self):
        """Start the monitoring loop"""
        self.running = True
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        print(f"✅ Started playlist monitoring with {self.config.CHECK_INTERVAL}s interval")

    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.running = False
        print("✅ Playlist monitoring stopped")

    def _monitor_loop(self):
        """FIXED: Interval monitor that properly skips when other operations are running"""
        while self.running:
            try:
                # FIXED: Check if ANY operation is running before starting
                with self._master_lock:
                    if self._is_checking or self._is_initial_checking or self._is_downloading:
                        operation_type = (
                            "interval check" if self._is_checking else
                            "initial import" if self._is_initial_checking else
                            "downloading" if self._is_downloading else "unknown"
                        )
                        print(f"⚠️ [MONITOR] Scheduled check skipped - {operation_type} in progress")
                        time.sleep(self.config.CHECK_INTERVAL)
                        continue

                    # Mark that interval check is starting
                    self._is_checking = True
                    print("🔄 [MONITOR] Starting scheduled interval check")

                try:
                    total_new = self.check_all_playlists()
                    print(f"✅ [MONITOR] Scheduled check completed: {total_new} new videos")
                finally:
                    # Always clear the checking flag
                    with self._master_lock:
                        self._is_checking = False

                time.sleep(self.config.CHECK_INTERVAL)

            except Exception as e:
                print(f"❌ [MONITOR] Error in monitoring loop: {e}")
                with self._master_lock:
                    self._is_checking = False
                time.sleep(60)  # Wait before retry on error

    def trigger_manual_check(self):
        """FIXED: Manual check that properly respects other operations"""
        with self._master_lock:
            if self._is_checking or self._is_initial_checking or self._is_downloading:
                operation_type = (
                    "interval monitor" if self._is_checking else
                    "initial import" if self._is_initial_checking else
                    "downloading" if self._is_downloading else "unknown"
                )
                return {
                    "success": False,
                    "message": f"Check already in progress ({operation_type}). Please wait...",
                    "status": "already_running"
                }

            self._is_checking = True
            print("🔄 [MANUAL] Manual check triggered")

        try:
            total_new = self.check_all_playlists()
            return {
                "success": True,
                "message": f"Manual check completed. Found {total_new} new songs.",
                "new_songs": total_new,
                "status": "completed"
            }
        except Exception as e:
            print(f"❌ [MANUAL] Manual check failed: {e}")
            return {
                "success": False,
                "message": f"Check failed: {str(e)}",
                "status": "failed"
            }
        finally:
            with self._master_lock:
                self._is_checking = False

    def check_all_playlists(self):
        """Check all active playlists for new videos"""
        playlists = self.db_manager.get_active_playlists()
        total_new = 0

        for playlist in playlists:
            print(f"🔍 [CHECK] Checking playlist: {playlist['name'] or playlist['url']}")
            new_count = self.check_playlist(playlist)
            total_new += new_count

        return total_new

    def perform_full_playlist_import(self, playlist_id: int, playlist_url: str):
        """FIXED: Initial import that properly blocks all other operations"""
        with self._master_lock:
            if self._is_checking or self._is_initial_checking or self._is_downloading:
                operation_type = (
                    "interval monitor" if self._is_checking else
                    "another initial import" if self._is_initial_checking else
                    "downloading" if self._is_downloading else "unknown"
                )
                print(f"⚠️ [IMPORT] Another operation in progress ({operation_type}), waiting...")
                return 0, 0, 1

            self._is_initial_checking = True
            print(f"🚀 [IMPORT] Starting full dual-source import for playlist {playlist_id}")

        try:
            # Step 1: Get dual-source playlist data
            playlist_info = self.downloader.get_playlist_dual_source(playlist_url)
            if not playlist_info or not playlist_info.get('entries'):
                raise Exception("No tracks found in playlist")

            # Step 2: Prepare and batch insert video data WITHOUT downloading
            videos_data = []
            for entry in playlist_info['entries']:
                if not entry.get('id'):
                    continue

                video_data = {
                    'video_id': entry['id'],
                    'title': entry.get('title', 'Unknown Title'),
                    'uploader': entry.get('artist', entry.get('uploader', 'Unknown Artist')),
                    'duration': entry.get('duration', 0),
                    'upload_date': entry.get('upload_date', ''),
                    'playlist_id': playlist_id,
                    'metadata': {
                        'album': entry.get('album', 'Unknown Album'),
                        'year': entry.get('year'),
                        'thumbnail': entry.get('thumbnail'),
                        'source': 'dual_import',
                        'availability': entry.get('availability', 'public')
                    },
                    'status': 'pending'
                }
                videos_data.append(video_data)

            # Step 3: Batch upsert to database
            inserted_count = self.db_manager.upsert_videos_batch(videos_data)
            print(f"✅ [IMPORT] Stored {inserted_count} tracks in database")

            # FIXED: Mark as downloading to prevent conflicts during download phase
            with self._master_lock:
                self._is_downloading = True

            try:
                # Step 4: Process downloads for pending videos
                downloaded_count = self.process_pending_downloads(playlist_id)
                print(f"✅ [IMPORT] Complete: {inserted_count} tracks stored, {downloaded_count} downloaded")
                return len(videos_data), downloaded_count, 0
            finally:
                with self._master_lock:
                    self._is_downloading = False

        except Exception as e:
            print(f"❌ [IMPORT] Failed: {e}")
            return 0, 0, 1
        finally:
            with self._master_lock:
                self._is_initial_checking = False

    def process_pending_downloads(self, playlist_id: int = None, max_concurrent: int = 1):
        """FIXED: Process downloads with proper concurrency control"""
        # Get pending videos that aren't being processed
        pending_videos = self.db_manager.get_videos_by_status('pending', playlist_id)
        
        if not pending_videos:
            print("📭 [DOWNLOAD] No pending downloads")
            return 0

        print(f"📋 [DOWNLOAD] Processing {len(pending_videos)} pending downloads")
        downloaded_count = 0

        for i, video in enumerate(pending_videos):
            video_id = video['video_id']

            # FIXED: Skip if already being processed or status changed
            with self._processing_lock:
                if video_id in self._processing_videos:
                    print(f"⚠️ [DOWNLOAD] Video {video_id} already being processed, skipping")
                    continue

                # Double-check database status
                current_status = self.db_manager.get_video_status(video_id)
                if current_status != 'pending':
                    print(f"⚠️ [DOWNLOAD] Video {video_id} status changed to '{current_status}', skipping")
                    continue

                # Add to processing set
                self._processing_videos.add(video_id)

            print(f"[{i+1}/{len(pending_videos)}] Downloading: {video['title']} ({video_id})")

            try:
                # Update status to processing
                self.db_manager.update_video_status(video_id, 'processing')

                # Perform download using database metadata
                result = self.downloader.download_video(
                    f"https://www.youtube.com/watch?v={video_id}",
                    video_id,
                    playlist_id
                )

                if result and result.get('status') == 'downloaded':
                    # FIXED: Check for duplicates BEFORE updating database
                    file_hash = result.get('file_hash')
                    if file_hash:
                        existing_file = self.db_manager.get_file_by_hash(file_hash)
                        if existing_file and existing_file.get('video_id') != video_id:
                            print(f"🔍 [DUPLICATE] Detected duplicate of existing file: {existing_file.get('video_id')}")
                            
                            # Remove newly downloaded file
                            if result.get('file_path') and os.path.exists(result['file_path']):
                                try:
                                    os.remove(result['file_path'])
                                    print(f"🗑️ [DUPLICATE] Removed duplicate file")
                                except Exception as e:
                                    print(f"Error removing duplicate file: {e}")
                            
                            # Mark as duplicate, reference existing file
                            result['status'] = 'duplicate'
                            result['file_path'] = existing_file.get('file_path', '')

                    # Update database with results
                    self.db_manager.update_video_with_download_result(video_id, result)
                    
                    if result.get('status') == 'downloaded':
                        downloaded_count += 1
                        print(f"✅ [DOWNLOAD] Successfully downloaded: {video['title']}")
                    else:
                        print(f"📋 [DUPLICATE] Marked as duplicate: {video['title']}")
                else:
                    # Mark as failed
                    self.db_manager.update_video_status(video_id, 'failed')
                    print(f"❌ [DOWNLOAD] Failed: {video['title']}")

            except Exception as e:
                print(f"❌ [DOWNLOAD] Error downloading {video_id}: {e}")
                self.db_manager.update_video_status(video_id, 'failed')
            finally:
                # FIXED: Always remove from processing set
                with self._processing_lock:
                    self._processing_videos.discard(video_id)

        return downloaded_count

    def check_playlist(self, playlist: dict):
        """FIXED: Check playlist - only add to database, don't download immediately"""
        try:
            # Use dual-source method for playlist checking
            playlist_info = self.downloader.get_playlist_dual_source(playlist['url'])
            if not playlist_info or not playlist_info.get('entries'):
                print(f"❌ [CHECK] No entries found for playlist: {playlist['url']}")
                return 0

            new_videos = 0
            skipped_videos = 0

            # FIXED: Only add to database, don't download during check
            for entry in playlist_info['entries']:
                if not isinstance(entry, dict) or not entry.get('id'):
                    continue

                video_id = entry['id']

                # Check if already successfully downloaded or exists
                if self.db_manager.video_exists(video_id):
                    skipped_videos += 1
                    continue

                # Check if already in database (any status)
                if self.db_manager.video_in_database(video_id):
                    skipped_videos += 1
                    continue

                print(f"🆕 [CHECK] New video found: {entry['title']} ({video_id})")

                # FIXED: Only add to database with 'pending' status - don't download
                try:
                    video_data = {
                        'video_id': video_id,
                        'title': entry.get('title', 'Unknown Title'),
                        'uploader': entry.get('artist', entry.get('uploader', 'Unknown Artist')),
                        'duration': entry.get('duration', 0),
                        'upload_date': entry.get('upload_date', ''),
                        'playlist_id': playlist['id'],
                        'metadata': {
                            'album': entry.get('album', 'Unknown Album'),
                            'year': entry.get('year'),
                            'thumbnail': entry.get('thumbnail'),
                            'source': 'playlist_check'
                        },
                        'status': 'pending'
                    }

                    if self.db_manager.add_video(video_data):
                        new_videos += 1
                        print(f"✅ [CHECK] Added to database: {entry['title']}")

                except Exception as e:
                    print(f"❌ [CHECK] Error adding new video {video_id}: {e}")

            # FIXED: Only process pending downloads if no other operation is running
            with self._master_lock:
                if not (self._is_checking or self._is_initial_checking):
                    pending_videos = self.db_manager.get_videos_by_status('pending', playlist['id'])
                    if pending_videos:
                        print(f"📋 [CHECK] Found {len(pending_videos)} pending videos to download")
                        self._is_downloading = True

            try:
                if not (self._is_checking or self._is_initial_checking):
                    pending_downloaded = self.process_pending_downloads(playlist['id'])
                    print(f"✅ [CHECK] Downloaded {pending_downloaded} pending videos")
            finally:
                with self._master_lock:
                    self._is_downloading = False

            self.db_manager.update_playlist_check_time(playlist['id'])
            print(f"✅ [CHECK] Playlist check complete: {new_videos} new, {skipped_videos} skipped")

            return new_videos

        except Exception as e:
            print(f"❌ [CHECK] Error checking playlist: {e}")
            return 0

    # Legacy method for backward compatibility
    def perform_initial_playlist_check(self, playlist_id: int, playlist_info: dict):
        """Legacy method - redirects to new dual-source import"""
        playlists = self.db_manager.get_active_playlists()
        playlist_url = None
        for p in playlists:
            if p['id'] == playlist_id:
                playlist_url = p['url']
                break

        if playlist_url:
            return self.perform_full_playlist_import(playlist_id, playlist_url)
        else:
            print(f"❌ Could not find playlist URL for ID {playlist_id}")
            return 0, 0, 1
