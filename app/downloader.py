import os
import re
import requests
import yt_dlp
import hashlib
import time
import random
import json
import sqlite3
from typing import Dict, Optional
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

        # Initialize YouTube Music API with Hong Kong localization
        self.ytmusic = YTMusic(language='zh_TW', location='HK')

        self.base_ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(download_dir, '%(title)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '0',
            }],
            'ignoreerrors': True,
            'no_warnings': False,
            'quiet': False,
        }

        # Remove cache - we use database instead
        # self._playlist_cache = {}

    def get_playlist_info(self, playlist_url: str) -> Dict:
        """Extract playlist information using ytmusicapi with HK localization"""
        print(f"🔍 Extracting playlist with ytmusicapi (HK): {playlist_url}")
        try:
            # Extract playlist ID from URL
            playlist_id = self._extract_playlist_id(playlist_url)
            if not playlist_id:
                raise ValueError("Invalid playlist URL")

            # Get playlist info with Hong Kong localization
            playlist_info = self.ytmusic.get_playlist(playlist_id, limit=None)
            if not playlist_info or not playlist_info.get('tracks'):
                print(f"❌ No tracks found in playlist")
                return self._fallback_playlist_extraction(playlist_url)

            # NO CACHING - Convert ytmusicapi format to our expected format
            entries = []
            for track in playlist_info['tracks']:
                if not track.get('videoId'):
                    continue
                video_entry = {
                    'id': track['videoId'],
                    'title': track.get('title', 'Unknown Title'),
                    'url': f"https://www.youtube.com/watch?v={track['videoId']}",
                    'availability': 'public',
                    'duration': track.get('duration_seconds', 0),
                    'uploader': self._extract_artist_name(track),
                    'upload_date': '',
                    'album': track.get('album', {}).get('name') if track.get('album') else 'Unknown Album',
                    'artist': self._extract_artist_name(track),
                    'thumbnail': track.get('thumbnails', [{}])[-1].get('url') if track.get('thumbnails') else None,
                    'year': track.get('year', None)
                }
                entries.append(video_entry)

            print(f"✅ Successfully extracted {len(entries)} tracks with HK metadata")
            return {
                'title': playlist_info.get('title', 'Unknown Playlist'),
                'entries': entries,
                'playlist_id': playlist_id
            }

        except Exception as e:
            print(f"❌ ytmusicapi extraction failed: {e}")
            # Fallback to yt-dlp extraction
            return self._fallback_playlist_extraction(playlist_url)

    def download_video_from_database(self, video_url: str, video_id: str, playlist_id: str = None) -> Optional[Dict]:
        """Download video using metadata from DATABASE (no cache)"""
        print(f"🎵 Downloading video from database metadata: {video_id}")
        clean_url = video_url.replace('music.youtube.com', 'www.youtube.com')
        safe_title = self._sanitize_filename(f"video_{video_id}")

        try:
            # Step 1: Get yt-dlp technical info (for duration, availability, etc.)
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                tech_info = ydl.extract_info(clean_url, download=False)
            if not tech_info:
                print(f"❌ Could not get technical info for {video_id}")
                return None
            availability = tech_info.get('availability', 'public')
            if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                print(f"🔒 Video requires authentication: {availability}")
                return None

            # Step 2: GET METADATA FROM DATABASE (no cache)
            title = f'video_{video_id}'  # Default fallback
            album_name = 'Unknown Album'
            artist = 'Unknown Artist'
            year = None
            thumbnail_url = None

            # Get metadata from database
            try:
                with sqlite3.connect(self.config.DATABASE_PATH) as conn:
                    cursor = conn.execute('SELECT metadata FROM videos WHERE video_id = ?', (video_id,))
                    row = cursor.fetchone()
                    if row and row[0]:
                        db_metadata = json.loads(row[0])
                        title = db_metadata.get('title', title)
                        artist = db_metadata.get('artist', artist)
                        album_name = db_metadata.get('album', album_name)
                        year = db_metadata.get('year')
                        thumbnail_url = db_metadata.get('thumbnail')
                        
                        print(f"✅ Using DATABASE metadata:")
                        print(f" Title: {title}")
                        print(f" Artist: {artist}")
                        print(f" Album: {album_name}")
                        print(f" Year: {year}")
                    else:
                        print(f"⚠️ No database metadata found, using yt-dlp fallback")
                        title = tech_info.get('title', title)
                        artist = tech_info.get('uploader', artist)
            except Exception as e:
                print(f"Error getting database metadata: {e}")
                title = tech_info.get('title', title)
                artist = tech_info.get('uploader', artist)

            # Clean title for filename
            clean_title = self._sanitize_filename(title)

            # Step 3: Download with cleaned filename
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

            # Step 4: Find downloaded file and add metadata
            actual_file_path = self._find_downloaded_file(clean_title, safe_title)
            if actual_file_path and os.path.exists(actual_file_path):
                file_size = os.path.getsize(actual_file_path)
                file_hash = self._calculate_file_hash(actual_file_path)

                # Use database metadata for tagging
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

                print(f"✅ Downloaded with database metadata: {title} ({file_size} bytes)")

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
                        'localization': 'HK_ytmusicapi_from_database'
                    }
                }
            else:
                print(f"❌ File not found after download for {video_id}")
                return None

        except Exception as e:
            print(f"❌ Download failed for {video_id}: {e}")
            return None

    def download_video(self, video_url: str, video_id: str, playlist_id: str = None) -> Optional[Dict]:
        """Download video - Uses database first, fallback to old method if needed"""
        # Try database method first
        try:
            with sqlite3.connect(self.config.DATABASE_PATH) as conn:
                cursor = conn.execute('SELECT metadata FROM videos WHERE video_id = ?', (video_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    # Database metadata exists, use it
                    return self.download_video_from_database(video_url, video_id, playlist_id)
        except Exception as e:
            print(f"Database check failed: {e}")

        # Fallback to original method for backwards compatibility
        return self._download_video_original(video_url, video_id, playlist_id)

    def _download_video_original(self, video_url: str, video_id: str, playlist_id: str = None) -> Optional[Dict]:
        """Original download method as fallback"""
        print(f"🎵 Downloading video (fallback method): {video_id}")
        clean_url = video_url.replace('music.youtube.com', 'www.youtube.com')
        safe_title = self._sanitize_filename(f"video_{video_id}")

        try:
            # Step 1: Get yt-dlp technical info (for duration, availability, etc.)
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                tech_info = ydl.extract_info(clean_url, download=False)
            if not tech_info:
                print(f"❌ Could not get technical info for {video_id}")
                return None
            availability = tech_info.get('availability', 'public')
            if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                print(f"🔒 Video requires authentication: {availability}")
                return None

            # Step 2: Use basic yt-dlp metadata
            title = tech_info.get('title', f'video_{video_id}')
            artist = tech_info.get('uploader', 'Unknown Artist')
            album_name = 'Unknown Album'
            year = None
            thumbnail_url = tech_info.get('thumbnail')

            print(f"⚠️ Using yt-dlp fallback metadata:")
            print(f" Title: {title}")
            print(f" Artist: {artist}")

            # Clean title for filename
            clean_title = self._sanitize_filename(title)

            # Step 3: Download with cleaned filename
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

            # Step 4: Find downloaded file and add metadata
            actual_file_path = self._find_downloaded_file(clean_title, safe_title)
            if actual_file_path and os.path.exists(actual_file_path):
                file_size = os.path.getsize(actual_file_path)
                file_hash = self._calculate_file_hash(actual_file_path)

                # Use basic metadata for tagging
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

                print(f"✅ Downloaded with fallback metadata: {title} ({file_size} bytes)")

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
                        'localization': 'fallback_ytdlp'
                    }
                }
            else:
                print(f"❌ File not found after download for {video_id}")
                return None

        except Exception as e:
            print(f"❌ Download failed for {video_id}: {e}")
            return None

    def _add_mp3_metadata(self, file_path: str, info: Dict, album_name: str, artist: str, year: str = None):
        """Add metadata and cover image to MP3 file with HK localization"""
        try:
            if not file_path or not file_path.lower().endswith('.mp3'):
                return

            audio_file = MP3(file_path, ID3=ID3)
            if audio_file.tags is None:
                audio_file.add_tags()

            # Title (Hong Kong localized)
            title = info.get('title')
            if title and isinstance(title, str):
                audio_file.tags.add(TIT2(encoding=3, text=title))

            # Artist (Hong Kong localized)
            if artist and isinstance(artist, str):
                audio_file.tags.add(TPE1(encoding=3, text=artist))

            # Album (Hong Kong localized)
            audio_file.tags.add(TALB(encoding=3, text=album_name))

            # Year (Hong Kong localized or from upload date)
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
                        # Determine MIME type
                        mime_type = 'image/jpeg'
                        if thumb_url.lower().endswith('.png'):
                            mime_type = 'image/png'
                        elif thumb_url.lower().endswith('.webp'):
                            mime_type = 'image/webp'
                        audio_file.tags.add(
                            APIC(
                                encoding=3,
                                mime=mime_type,
                                type=3,  # Front cover
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
            print(f"🏷️ HK metadata added: {title} | {artist} | {album_name} | {year}")

        except Exception as e:
            print(f"❌ Metadata error: {e}")

    def _extract_playlist_id(self, url: str) -> Optional[str]:
        """Extract playlist ID from YouTube URL"""
        pattern = r'[&?]list=([^&]+)'
        match = re.search(pattern, url)
        return match.group(1) if match else None

    def _extract_artist_name(self, track: dict) -> str:
        """Extract artist name from track data"""
        artists = track.get('artists', [])
        if artists and len(artists) > 0:
            return artists[0].get('name', 'Unknown Artist')
        return 'Unknown Artist'

    def _fallback_playlist_extraction(self, playlist_url: str) -> Dict:
        """Fallback to original yt-dlp extraction if ytmusicapi fails"""
        print("🔄 Falling back to yt-dlp for playlist extraction...")
        clean_url = playlist_url.replace('&amp;', '&').strip()
        ydl_opts = {
            'extract_flat': True,
            'quiet': True,
            'no_warnings': True,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(clean_url, download=False)
                if not info or not isinstance(info, dict):
                    print(f"❌ Invalid info returned")
                    return {'title': None, 'entries': []}
                entries_raw = info.get('entries', [])
                if not entries_raw:
                    print(f"❌ No entries found")
                    return {'title': None, 'entries': []}

                entries = []
                for entry in entries_raw:
                    if not isinstance(entry, dict) or not entry.get('id'):
                        continue
                    # Skip restricted content
                    availability = entry.get('availability', 'public')
                    if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                        continue
                    video_entry = {
                        'id': entry.get('id'),
                        'title': entry.get('title') or f"Video {entry.get('id')}",
                        'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                        'availability': availability,
                        'duration': entry.get('duration'),
                        'uploader': entry.get('uploader'),
                        'upload_date': entry.get('upload_date'),
                        'album': 'Unknown Album',
                        'artist': entry.get('uploader', 'Unknown Artist'),
                        'thumbnail': None,
                        'year': None
                    }
                    entries.append(video_entry)

                result = {'title': info.get('title', 'Unknown Playlist'), 'entries': entries}
                print(f"✅ Successfully extracted {len(entries)} videos")
                return result
        except Exception as e:
            print(f"❌ Fallback extraction also failed: {e}")
            return {'title': None, 'entries': []}

    def _find_downloaded_file(self, title: str, safe_title: str) -> Optional[str]:
        """Find the downloaded MP3 file"""
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
        """Clean filename for filesystem"""
        if not filename or not isinstance(filename, str):
            return 'unknown'
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        filename = ' '.join(filename.split())
        filename = filename[:200].strip('. ')
        return filename if filename else 'unknown'

    def _calculate_file_hash(self, file_path: str) -> Optional[str]:
        """Calculate file hash"""
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
