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
        print(f"✅ Started playlist monitoring with {self.config.CHECK_INTERVAL}s interval")

    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.running = False
        print("✅ Playlist monitoring stopped")

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
                    print(f"✅ Scheduled monitor check completed: {total_new} new videos")
                finally:
                    with self._check_lock:
                        self._is_checking = False

                time.sleep(self.config.CHECK_INTERVAL)
            except Exception as e:
                print(f"❌ Error in monitoring loop: {e}")
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
            print(f"❌ Manual check failed: {e}")
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
            print(f"🔍 Checking playlist: {playlist['name'] or playlist['url']}")
            new_count = self.check_playlist(playlist)
            total_new += new_count

        return total_new

    def perform_full_playlist_import(self, playlist_id: int, playlist_url: str):
        """Complete dual-source playlist import workflow"""
        with self._initial_check_lock:
            if self._is_initial_checking:
                print("⚠️ Another initial check in progress, queuing...")
                time.sleep(5)
                with self._initial_check_lock:
                    if self._is_initial_checking:
                        return 0, 0, 1
            self._is_initial_checking = True

        try:
            print(f"🚀 [IMPORT] Starting full dual-source import for playlist {playlist_id}")

            # Step 1: Get dual-source playlist data
            playlist_info = self.downloader.get_playlist_dual_source(playlist_url)
            
            if not playlist_info or not playlist_info.get('entries'):
                raise Exception("No tracks found in playlist")

            # Step 2: Prepare and batch insert video data
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

            # Step 4: Process downloads for pending videos
            downloaded_count = self.process_pending_downloads(playlist_id)

            print(f"✅ [IMPORT] Complete: {inserted_count} tracks stored, {downloaded_count} downloaded")
            return len(videos_data), downloaded_count, 0

        except Exception as e:
            print(f"❌ [IMPORT] Failed: {e}")
            return 0, 0, 1
        finally:
            with self._initial_check_lock:
                self._is_initial_checking = False

    def process_pending_downloads(self, playlist_id: int = None, max_concurrent: int = 3):
        """Process downloads for pending tracks only"""
        pending_videos = self.db_manager.get_videos_by_status('pending', playlist_id)
        
        if not pending_videos:
            print("📭 No pending downloads")
            return 0
            
        print(f"📋 Processing {len(pending_videos)} pending downloads")
        downloaded_count = 0
        
        for i, video in enumerate(pending_videos):
            video_id = video['video_id']
            
            # Check if already being processed (safety check)
            current_status = self.db_manager.get_video_status(video_id)
            if current_status != 'pending':
                continue
                
            print(f"[{i+1}/{len(pending_videos)}] Downloading: {video['title']} ({video_id})")
            
            # Update status to processing
            self.db_manager.update_video_status(video_id, 'processing')
            
            try:
                # Perform download using database metadata
                result = self.downloader.download_video(
                    f"https://www.youtube.com/watch?v={video_id}", 
                    video_id, 
                    playlist_id
                )
                
                if result and result.get('status') == 'downloaded':
                    # Update with download results
                    self.db_manager.update_video_with_download_result(video_id, result)
                    downloaded_count += 1
                    print(f"✅ Downloaded: {video['title']}")
                else:
                    # Mark as failed
                    self.db_manager.update_video_status(video_id, 'failed')
                    print(f"❌ Failed: {video['title']}")
                    
            except Exception as e:
                print(f"❌ Error downloading {video_id}: {e}")
                self.db_manager.update_video_status(video_id, 'failed')
        
        return downloaded_count

    def check_playlist(self, playlist: dict):
        """Check a single playlist for new videos and download pending ones"""
        try:
            # Use dual-source method for playlist checking
            playlist_info = self.downloader.get_playlist_dual_source(playlist['url'])
            
            if not playlist_info or not playlist_info.get('entries'):
                print(f"❌ No entries found for playlist: {playlist['url']}")
                return 0

            new_videos = 0
            skipped_videos = 0

            # Check for new videos from playlist
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
                    # Add to database if not exists
                    if not self.db_manager.video_in_database(video_id):
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
                        self.db_manager.add_video(video_data)

                    # Update status to processing and download
                    self.db_manager.update_video_status(video_id, 'processing')
                    
                    download_result = self.downloader.download_video(entry['url'], video_id, playlist['id'])
                    
                    if download_result:
                        # Check for duplicates by hash
                        if download_result.get('file_hash'):
                            existing_file = self.db_manager.get_file_by_hash(download_result['file_hash'])
                            if existing_file:
                                print(f"🔍 Duplicate detected: {download_result['title']}")
                                if download_result.get('file_path') and os.path.exists(download_result['file_path']):
                                    try:
                                        os.remove(download_result['file_path'])
                                        print(f"🗑️ Removed duplicate file")
                                    except Exception as e:
                                        print(f"Error removing duplicate file: {e}")
                                download_result['status'] = 'duplicate'
                                download_result['file_path'] = existing_file['file_path']

                        # Update database with results
                        self.db_manager.update_video_with_download_result(video_id, download_result)
                        
                        self.db_manager.log_download_action(
                            video_id,
                            download_result.get('status', 'processed'),
                            f"Downloaded from playlist {playlist['name']}",
                            playlist['id']
                        )

                        if download_result.get('status') == 'downloaded':
                            new_videos += 1
                            print(f"✅ Successfully downloaded: {download_result['title']}")
                    else:
                        self.db_manager.update_video_status(video_id, 'failed')

                except Exception as e:
                    print(f"❌ Error processing new video {video_id}: {e}")
                    self.db_manager.update_video_status(video_id, 'failed')
                finally:
                    # Remove from processing set
                    with self._processing_lock:
                        self._processing_videos.discard(video_id)

            # Process any remaining pending videos for this playlist
            pending_videos = self.db_manager.get_videos_by_status('pending', playlist['id'])
            if pending_videos:
                print(f"📋 Found {len(pending_videos)} pending videos to download")
                pending_downloaded = self.process_pending_downloads(playlist['id'])
                new_videos += pending_downloaded

            self.db_manager.update_playlist_check_time(playlist['id'])
            print(f"✅ Playlist check complete: {new_videos} new, {skipped_videos} skipped")
            return new_videos

        except Exception as e:
            print(f"❌ Error checking playlist: {e}")
            return 0

    # Legacy method for backward compatibility
    def perform_initial_playlist_check(self, playlist_id: int, playlist_info: dict):
        """Legacy method - redirects to new dual-source import"""
        playlist = self.db_manager.get_active_playlists()
        playlist_url = None
        
        for p in playlist:
            if p['id'] == playlist_id:
                playlist_url = p['url']
                break
                
        if playlist_url:
            return self.perform_full_playlist_import(playlist_id, playlist_url)
        else:
            print(f"❌ Could not find playlist URL for ID {playlist_id}")
            return 0, 0, 1
