import os
import yt_dlp
import hashlib
import time
import random
from typing import Dict, Optional
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC
from config import Config

class YouTubeDownloader:
    def __init__(self, download_dir: str, audio_quality: str = '320'):
        self.download_dir = download_dir
        self.audio_quality = audio_quality
        self.config = Config()  # Add config instance
        os.makedirs(download_dir, exist_ok=True)

        # Correct yt-dlp options based on official docs
        self.base_ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(download_dir, '%(title)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '0',  # Best quality auto-determined
            }],
            'ignoreerrors': True,
            'no_warnings': False,
            'quiet': False,
        }

    def get_playlist_info(self, playlist_url: str) -> Dict:
        """Extract playlist information"""
        clean_url = playlist_url.replace('&amp;', '&').strip()
        print(f"🔍 Extracting playlist: {clean_url}")
        
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
                
                print(f"📊 Found {len(entries_raw)} raw entries")
                
                entries = []
                for i, entry in enumerate(entries_raw):
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
                        'upload_date': entry.get('upload_date')
                    }
                    entries.append(video_entry)
                    
                    if i < 3:
                        print(f"✅ Entry {i+1}: {video_entry['title']}")
                
                result = {'title': info.get('title', 'Unknown Playlist'), 'entries': entries}
                print(f"✅ Successfully extracted {len(entries)} videos")
                return result
                
        except Exception as e:
            print(f"❌ Failed to extract playlist: {e}")
            return {'title': None, 'entries': []}

    def download_video(self, video_url: str, video_id: str) -> Optional[Dict]:
        """Download video with original language title and proper album metadata"""
        print(f"🎵 Downloading video: {video_id}")
        
        clean_url = video_url.replace('music.youtube.com', 'www.youtube.com')
        safe_title = self._sanitize_filename(f"video_{video_id}")

        try:
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
                # Extract info first
                print(f"📋 Extracting info for {video_id}...")
                info = ydl.extract_info(clean_url, download=False)

                if not info or not isinstance(info, dict):
                    print(f"❌ Invalid info for {video_id}")
                    return None

                # Check availability
                availability = info.get('availability', 'public')
                if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                    print(f"🔒 Video {video_id} requires authentication: {availability}")
                    return None

                # Get original title and metadata
                title = info.get('title', f'video_{video_id}')
                album_name = info.get('album') or info.get('playlist_title') or 'Unknown Album'
                uploader = info.get('uploader', 'Unknown Artist')
                
                print(f"📥 Downloading: {title}")
                print(f"🏷️ Album: {album_name}")

                # yt-dlp options with proper metadata
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'outtmpl': os.path.join(self.download_dir, '%(title)s.%(ext)s'),  # Original title
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '0',
                    }],
                    'postprocessor_args': [
                        '-metadata', f'album={album_name}',
                        '-metadata', f'artist={uploader}',
                        '-metadata', f'title={title}'
                    ],
                    'ignoreerrors': True,
                    'no_warnings': False,
                    'quiet': False,
                }

                # Download the video
                with yt_dlp.YoutubeDL(ydl_opts) as download_ydl:
                    download_ydl.download([clean_url])

                # Find downloaded file
                actual_file_path = self._find_downloaded_file(title, safe_title)

                if actual_file_path and os.path.exists(actual_file_path):
                    file_size = os.path.getsize(actual_file_path)
                    file_hash = self._calculate_file_hash(actual_file_path)

                    # Add additional metadata with mutagen (using dynamic album name)
                    self._add_mp3_metadata(actual_file_path, info, album_name)

                    print(f"✅ Successfully downloaded: {title} ({file_size} bytes)")

                    # SUCCESS: Configurable wait before next download
                    if self.config.DOWNLOAD_DELAY_ENABLED:
                        wait_time = random.uniform(self.config.DOWNLOAD_DELAY_MIN, self.config.DOWNLOAD_DELAY_MAX)
                        print(f"⏰ SUCCESS - Delay enabled, waiting {wait_time:.1f} seconds before next download...")
                        time.sleep(wait_time)
                    else:
                        print(f"⚡ SUCCESS - Delay disabled, continuing immediately")

                    return {
                        'video_id': video_id,
                        'title': title,
                        'uploader': uploader,
                        'duration': info.get('duration', 0),
                        'upload_date': info.get('upload_date', ''),
                        'file_path': actual_file_path,
                        'file_hash': file_hash,
                        'file_size': file_size,
                        'status': 'downloaded',
                        'metadata': {
                            'description': info.get('description', ''),
                            'view_count': info.get('view_count', 0),
                            'album': album_name
                        }
                    }
                else:
                    print(f"❌ File not found after download for {video_id}")
                    return None

        except Exception as e:
            print(f"❌ Download failed for {video_id}: {e}")
            return None



    def _find_downloaded_file(self, title: str, safe_title: str) -> Optional[str]:
        """Find the downloaded MP3 file"""
        # yt-dlp with FFmpegExtractAudio creates .mp3 files
        possible_names = [
            self._sanitize_filename(title) + '.mp3',
            safe_title + '.mp3',
            f"video_{safe_title}.mp3"
        ]
        
        for name in possible_names:
            full_path = os.path.join(self.download_dir, name)
            if os.path.exists(full_path):
                return full_path
        
        # Search directory for any MP3 files containing the video ID or title
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
        
        invalid_chars = '<>:"/\\|?*'
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

    def _add_mp3_metadata(self, file_path: str, info: Dict, album_name: str):
        """Add metadata to MP3 file with proper album name"""
        try:
            if not file_path or not file_path.lower().endswith('.mp3'):
                return

            if not isinstance(info, dict):
                return

            audio_file = MP3(file_path, ID3=ID3)
            if audio_file.tags is None:
                audio_file.add_tags()

            # Safe metadata extraction with original language
            title = info.get('title')
            if title and isinstance(title, str):
                audio_file.tags.add(TIT2(encoding=3, text=title))

            uploader = info.get('uploader')
            if uploader and isinstance(uploader, str):
                audio_file.tags.add(TPE1(encoding=3, text=uploader))

            # Use dynamic album name instead of hardcoded 'YouTube Download'
            audio_file.tags.add(TALB(encoding=3, text=album_name))

            upload_date = info.get('upload_date')
            if upload_date and isinstance(upload_date, str) and len(upload_date) >= 4:
                year = upload_date[:4]
                audio_file.tags.add(TDRC(encoding=3, text=year))

            audio_file.save()
            print(f"🏷️ Metadata added to: {os.path.basename(file_path)} (Album: {album_name})")

        except Exception as e:
            print(f"Metadata error (non-critical): {e}")


