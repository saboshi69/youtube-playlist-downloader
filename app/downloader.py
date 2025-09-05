import os
import re
import requests
import yt_dlp
import hashlib
import time
import random
import json
import sqlite3
from typing import Dict, Optional, List
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB, TDRC
from ytmusicapi import YTMusic
from config import Config

class YouTubeDownloader:

    def __init__(self, download_dir: str, audio_quality: str = '320'):
        self.download_dir = download_dir
        self.audio_quality = audio_quality
        self.config = Config()
        os.makedirs(download_dir, exist_ok=True)
        
        try:
            self.ytmusic = YTMusic(language='zh_TW', location='HK')
            print("✅ YTMusic API initialized with HK localization")
        except Exception as e:
            print(f"❌ CRITICAL: YTMusic API failed to initialize: {e}")
            self.ytmusic = None
            raise Exception("Cannot proceed without YTMusic API")

    def get_playlist_info_batch(self, playlist_url: str) -> Dict:
        """BATCH FIRST: Get entire playlist data in one call, NO yt-dlp fallback for playlist"""
        print(f"🔍 [BATCH] Extracting playlist with ytmusicapi: {playlist_url}")
        
        if not self.ytmusic:
            raise Exception("❌ YTMusic API not available - Cannot fetch playlist")
        
        try:
            playlist_id = self._extract_playlist_id(playlist_url)
            if not playlist_id:
                raise ValueError("Invalid playlist URL")

            # BATCH FETCH: Get entire playlist at once
            playlist_info = self.ytmusic.get_playlist(playlist_id, limit=None)
            
            if not playlist_info or not playlist_info.get('tracks'):
                raise Exception("No tracks found in playlist")

            print(f"✅ [BATCH] Successfully fetched {len(playlist_info['tracks'])} tracks")
            
            # Convert to standard format
            entries = []
            for i, track in enumerate(playlist_info['tracks']):
                try:
                    if not track or not isinstance(track, dict) or not track.get('videoId'):
                        print(f"⚠️ [BATCH] Skipping invalid track {i}")
                        continue
                    
                    video_entry = {
                        'id': track['videoId'],
                        'title': track.get('title', 'Unknown Title'),
                        'url': f"https://www.youtube.com/watch?v={track['videoId']}",
                        'availability': 'public',
                        'duration': track.get('duration_seconds', 0),
                        'uploader': self._extract_artist_name_safe(track),
                        'upload_date': '',
                        'album': self._extract_album_name_safe(track),
                        'artist': self._extract_artist_name_safe(track),
                        'thumbnail': self._extract_thumbnail_safe(track),
                        'year': self._extract_year_safe(track)
                    }
                    entries.append(video_entry)
                    
                except Exception as track_error:
                    print(f"⚠️ [BATCH] Error processing track {i}: {track_error}")
                    continue

            return {
                'title': playlist_info.get('title', 'Unknown Playlist'),
                'entries': entries,
                'playlist_id': playlist_id
            }

        except Exception as e:
            # NO FALLBACK TO YT-DLP FOR PLAYLIST - Just fail and report error
            print(f"❌ [BATCH] Playlist fetch completely failed: {e}")
            raise Exception(f"Failed to fetch playlist: {e}")

    def enrich_missing_metadata(self, video_id: str, existing_metadata: Dict) -> Dict:
        """Enrich missing metadata using ytmusicapi get_song, then yt-dlp fallback"""
        print(f"🔍 [ENRICH] Checking metadata completeness for {video_id}")
        
        # Check what's missing
        missing_fields = []
        if not existing_metadata.get('title') or existing_metadata.get('title') == 'Unknown Title':
            missing_fields.append('title')
        if not existing_metadata.get('artist') or existing_metadata.get('artist') == 'Unknown Artist':
            missing_fields.append('artist')
        if not existing_metadata.get('album') or existing_metadata.get('album') == 'Unknown Album':
            missing_fields.append('album')
        if not existing_metadata.get('year'):
            missing_fields.append('year')
        if not existing_metadata.get('thumbnail'):
            missing_fields.append('thumbnail')
        
        if not missing_fields:
            print(f"✅ [ENRICH] Metadata complete for {video_id}")
            return existing_metadata
        
        print(f"🔄 [ENRICH] Missing fields for {video_id}: {missing_fields}")
        enriched_metadata = existing_metadata.copy()
        
        # STEP 1: Try ytmusicapi get_song for missing fields
        if self.ytmusic:
            try:
                print(f"🎵 [ENRICH] Calling ytmusicapi.get_song for {video_id}")
                song_details = self.ytmusic.get_song(video_id)
                
                if song_details:
                    # Extract additional metadata from get_song
                    if 'title' in missing_fields and song_details.get('videoDetails', {}).get('title'):
                        enriched_metadata['title'] = song_details['videoDetails']['title']
                        missing_fields.remove('title')
                        print(f"✅ [ENRICH] Got title from get_song: {enriched_metadata['title']}")
                    
                    if 'artist' in missing_fields and song_details.get('videoDetails', {}).get('author'):
                        enriched_metadata['artist'] = song_details['videoDetails']['author']
                        missing_fields.remove('artist')
                        print(f"✅ [ENRICH] Got artist from get_song: {enriched_metadata['artist']}")
                        
                    # Try to get thumbnail
                    if 'thumbnail' in missing_fields:
                        thumbnails = song_details.get('videoDetails', {}).get('thumbnail', {}).get('thumbnails', [])
                        if thumbnails:
                            enriched_metadata['thumbnail'] = thumbnails[-1].get('url')
                            missing_fields.remove('thumbnail')
                            print(f"✅ [ENRICH] Got thumbnail from get_song")
                            
            except Exception as e:
                print(f"⚠️ [ENRICH] get_song failed for {video_id}: {e}")
        
        # STEP 2: Try yt-dlp for remaining missing fields
        if missing_fields:
            try:
                print(f"🔄 [ENRICH] Trying yt-dlp for remaining fields: {missing_fields}")
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                
                with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                    tech_info = ydl.extract_info(video_url, download=False)
                
                if tech_info:
                    if 'title' in missing_fields and tech_info.get('title'):
                        enriched_metadata['title'] = tech_info['title']
                        missing_fields.remove('title')
                        print(f"✅ [ENRICH] Got title from yt-dlp: {enriched_metadata['title']}")
                    
                    if 'artist' in missing_fields and tech_info.get('uploader'):
                        enriched_metadata['artist'] = tech_info['uploader']
                        missing_fields.remove('artist')
                        print(f"✅ [ENRICH] Got artist from yt-dlp: {enriched_metadata['artist']}")
                        
                    if 'thumbnail' in missing_fields and tech_info.get('thumbnail'):
                        enriched_metadata['thumbnail'] = tech_info['thumbnail']
                        missing_fields.remove('thumbnail')
                        print(f"✅ [ENRICH] Got thumbnail from yt-dlp")
                        
            except Exception as e:
                print(f"⚠️ [ENRICH] yt-dlp fallback failed for {video_id}: {e}")
        
        # STEP 3: Keep defaults for anything still missing
        if missing_fields:
            print(f"⚠️ [ENRICH] Still missing after all attempts: {missing_fields} - keeping defaults")
            
        return enriched_metadata

    def download_video(self, video_url: str, video_id: str, playlist_id: str = None) -> Optional[Dict]:
        """Download video - ALWAYS uses database metadata (from batch insert)"""
        print(f"🎵 [DOWNLOAD] Starting download for {video_id}")
        
        # Get metadata from database (should ALWAYS exist from batch insert)
        db_metadata = self._get_database_metadata(video_id)
        
        if not db_metadata:
            print(f"❌ [DOWNLOAD] No database metadata for {video_id} - this should not happen!")
            return None
            
        print(f"✅ [DOWNLOAD] Using database metadata for {video_id}")
        return self._perform_download_with_metadata(video_url, video_id, db_metadata)

    def _get_database_metadata(self, video_id: str) -> Optional[Dict]:
        """Get metadata from database"""
        try:
            with sqlite3.connect(self.config.DATABASE_PATH) as conn:
                cursor = conn.execute('SELECT metadata FROM videos WHERE video_id = ?', (video_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
                return None
        except Exception as e:
            print(f"Error getting database metadata: {e}")
            return None

    def _perform_download_with_metadata(self, video_url: str, video_id: str, metadata: Dict) -> Optional[Dict]:
        """Perform download using database metadata"""
        clean_url = video_url.replace('music.youtube.com', 'www.youtube.com')
        
        try:
            # Get technical info for availability check
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                tech_info = ydl.extract_info(clean_url, download=False)
            
            if not tech_info:
                print(f"❌ Could not get technical info for {video_id}")
                return None
            
            availability = tech_info.get('availability', 'public')
            if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                print(f"🔒 Video requires authentication: {availability}")
                return None

            # Use database metadata (enriched)
            title = metadata.get('title', f'video_{video_id}')
            artist = metadata.get('artist', 'Unknown Artist')
            album_name = metadata.get('album', 'Unknown Album')
            year = metadata.get('year')
            thumbnail_url = metadata.get('thumbnail')

            print(f"✅ [DOWNLOAD] Metadata: {title} | {artist} | {album_name}")

            # Clean title for filename
            clean_title = self._sanitize_filename(title)
            if clean_title == 'unknown' or clean_title.startswith('video_'):
                clean_title = self._sanitize_filename(f"{artist}_{title}"[:50])

            # Download
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': os.path.join(self.download_dir, f'{clean_title}.%(ext)s'),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '320',
                }],
                'ignoreerrors': True,
                'no_warnings': False,
                'quiet': False,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as download_ydl:
                download_ydl.download([clean_url])

            # Find and process downloaded file
            actual_file_path = self._find_downloaded_file(clean_title, f"video_{video_id}")
            if not actual_file_path or not os.path.exists(actual_file_path):
                print(f"❌ File not found after download for {video_id}")
                return None

            file_size = os.path.getsize(actual_file_path)
            file_hash = self._calculate_file_hash(actual_file_path)

            # Add metadata to MP3
            combined_info = {
                'title': title,
                'artist': artist,
                'album': album_name,
                'year': year,
                'thumbnail': thumbnail_url,
                'upload_date': tech_info.get('upload_date', ''),
                'duration': tech_info.get('duration', 0),
                'description': tech_info.get('description', ''),
                'view_count': tech_info.get('view_count', 0)
            }

            self._add_mp3_metadata(actual_file_path, combined_info, album_name, artist, year)

            print(f"✅ Downloaded: {title} ({file_size} bytes)")

            # Delay if configured
            if self.config.DOWNLOAD_DELAY_ENABLED:
                wait_time = random.uniform(self.config.DOWNLOAD_DELAY_MIN, self.config.DOWNLOAD_DELAY_MAX)
                print(f"⏰ Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)

            return {
                'video_id': video_id,
                'title': title,
                'uploader': artist,
                'duration': combined_info.get('duration', 0),
                'upload_date': combined_info.get('upload_date', ''),
                'file_path': actual_file_path,
                'file_hash': file_hash,
                'file_size': file_size,
                'status': 'downloaded',
                'metadata': {
                    'description': combined_info.get('description', ''),
                    'view_count': combined_info.get('view_count', 0),
                    'album': album_name,
                    'year': year,
                    'localization': 'HK_ytmusicapi_batch'
                }
            }

        except Exception as e:
            print(f"❌ Download failed for {video_id}: {e}")
            return None

    # Helper methods (same as before)
    def _extract_artist_name_safe(self, track: dict) -> str:
        try:
            artists = track.get('artists', [])
            if artists and isinstance(artists, list) and len(artists) > 0:
                first_artist = artists[0]
                if isinstance(first_artist, dict):
                    name = first_artist.get('name', 'Unknown Artist')
                    if name and isinstance(name, str):
                        return name
            return 'Unknown Artist'
        except Exception:
            return 'Unknown Artist'

    def _extract_album_name_safe(self, track: dict) -> str:
        try:
            album = track.get('album')
            if album and isinstance(album, dict):
                name = album.get('name', 'Unknown Album')
                if name and isinstance(name, str):
                    return name
            return 'Unknown Album'
        except Exception:
            return 'Unknown Album'

    def _extract_year_safe(self, track: dict):
        try:
            year = track.get('year')
            if year and isinstance(year, (int, str)):
                return str(year)
            return None
        except Exception:
            return None

    def _extract_thumbnail_safe(self, track: dict):
        try:
            thumbnails = track.get('thumbnails')
            if thumbnails and isinstance(thumbnails, list) and len(thumbnails) > 0:
                last_thumb = thumbnails[-1]
                if isinstance(last_thumb, dict):
                    url = last_thumb.get('url')
                    if url and isinstance(url, str):
                        return url
            return None
        except Exception:
            return None

    def _add_mp3_metadata(self, file_path: str, info: Dict, album_name: str, artist: str, year: str = None):
        try:
            if not file_path or not file_path.lower().endswith('.mp3'):
                return

            audio_file = MP3(file_path, ID3=ID3)
            if audio_file.tags is None:
                audio_file.add_tags()

            title = info.get('title')
            if title and isinstance(title, str):
                audio_file.tags.add(TIT2(encoding=3, text=title))

            if artist and isinstance(artist, str):
                audio_file.tags.add(TPE1(encoding=3, text=artist))

            audio_file.tags.add(TALB(encoding=3, text=album_name))

            if year:
                audio_file.tags.add(TDRC(encoding=3, text=str(year)))

            # Cover Art
            thumb_url = info.get('thumbnail')
            if thumb_url:
                try:
                    print(f"🖼️ Downloading cover image...")
                    response = requests.get(thumb_url, timeout=10)
                    if response.status_code == 200:
                        img_data = response.content
                        mime_type = 'image/jpeg'
                        if thumb_url.lower().endswith('.png'):
                            mime_type = 'image/png'
                        elif thumb_url.lower().endswith('.webp'):
                            mime_type = 'image/webp'
                        
                        audio_file.tags.add(
                            APIC(
                                encoding=3,
                                mime=mime_type,
                                type=3,
                                desc='Cover',
                                data=img_data
                            )
                        )
                        print(f"🖼️ Embedded cover image ({len(img_data)} bytes)")
                    else:
                        print(f"⚠️ Failed to download cover: HTTP {response.status_code}")
                except Exception as e:
                    print(f"⚠️ Could not embed cover image: {e}")

            audio_file.save()
            print(f"🏷️ Metadata added: {title} | {artist} | {album_name} | {year}")

        except Exception as e:
            print(f"❌ Metadata error: {e}")

    def _extract_playlist_id(self, url: str) -> Optional[str]:
        pattern = r'[&?]list=([^&]+)'
        match = re.search(pattern, url)
        return match.group(1) if match else None

    def _find_downloaded_file(self, title: str, safe_title: str) -> Optional[str]:
        possible_names = [
            self._sanitize_filename(title) + '.mp3',
            safe_title + '.mp3',
            f"video_{safe_title}.mp3"
        ]
        for name in possible_names:
            full_path = os.path.join(self.download_dir, name)
            if os.path.exists(full_path):
                return full_path
        try:
            for file in os.listdir(self.download_dir):
                if file.endswith('.mp3') and (safe_title in file or title[:20] in file):
                    return os.path.join(self.download_dir, file)
        except:
            pass
        return None

    def _sanitize_filename(self, filename: str) -> str:
        if not filename or not isinstance(filename, str):
            return 'unknown'
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        filename = ' '.join(filename.split())
        filename = filename[:200].strip('. ')
        return filename if filename else 'unknown'

    def _calculate_file_hash(self, file_path: str) -> Optional[str]:
        if not file_path or not os.path.exists(file_path):
            return None
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            print(f"Hash calculation error: {e}")
            return None
