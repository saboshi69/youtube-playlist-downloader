import yt_dlp
import os
import hashlib
from typing import Dict, List, Optional
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC
import json

class YouTubeDownloader:
    def __init__(self, download_dir: str, audio_quality: str = '320'):
        self.download_dir = download_dir
        self.audio_quality = audio_quality
        os.makedirs(download_dir, exist_ok=True)
    
    def get_playlist_info(self, playlist_url: str) -> Dict:
        """Extract playlist information without downloading"""
        ydl_opts = {
            'extract_flat': True,
            'quiet': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(playlist_url, download=False)
                return {
                    'title': info.get('title'),
                    'entries': [
                        {
                            'id': entry.get('id'),
                            'title': entry.get('title'),
                            'url': entry.get('url')
                        }
                        for entry in info.get('entries', [])
                        if entry and entry.get('id')
                    ]
                }
            except Exception as e:
                print(f"Error extracting playlist info: {e}")
                return {'title': None, 'entries': []}
    
    def download_video(self, video_url: str, video_id: str) -> Optional[Dict]:
        """Download a single video and return metadata"""
        filename_template = f'{self.download_dir}/%(title)s.%(ext)s'
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': filename_template,
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': self.audio_quality,
            'embed_metadata': True,
            'writeinfojson': False,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'ignoreerrors': True,
            'no_warnings': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info first
                info = ydl.extract_info(video_url, download=False)
                
                # Create safe filename
                safe_title = self._sanitize_filename(info.get('title', 'Unknown'))
                file_path = os.path.join(self.download_dir, f"{safe_title}.mp3")
                
                # Update template with safe filename
                ydl_opts['outtmpl'] = file_path.replace('.mp3', '.%(ext)s')
                
                # Download
                ydl.download([video_url])
                
                # Calculate file hash for duplicate detection
                file_hash = self._calculate_file_hash(file_path) if os.path.exists(file_path) else None
                
                # Add metadata to MP3 file
                if os.path.exists(file_path):
                    self._add_mp3_metadata(file_path, info)
                
                return {
                    'video_id': video_id,
                    'title': info.get('title'),
                    'uploader': info.get('uploader'),
                    'duration': info.get('duration'),
                    'upload_date': info.get('upload_date'),
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'metadata': {
                        'description': info.get('description'),
                        'view_count': info.get('view_count'),
                        'like_count': info.get('like_count'),
                        'tags': info.get('tags', []),
                        'thumbnail': info.get('thumbnail')
                    }
                }
        except Exception as e:
            print(f"Error downloading {video_id}: {e}")
            return None
    
    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:200]  # Limit length
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file for duplicate detection"""
        if not os.path.exists(file_path):
            return None
        
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _add_mp3_metadata(self, file_path: str, info: Dict):
        """Add metadata to MP3 file"""
        try:
            audio_file = MP3(file_path, ID3=ID3)
            
            # Add ID3 tag if it doesn't exist
            if audio_file.tags is None:
                audio_file.add_tags()
            
            # Add metadata
            audio_file.tags.add(TIT2(encoding=3, text=info.get('title', '')))
            audio_file.tags.add(TPE1(encoding=3, text=info.get('uploader', '')))
            audio_file.tags.add(TALB(encoding=3, text='YouTube Download'))
            
            if info.get('upload_date'):
                year = info['upload_date'][:4]
                audio_file.tags.add(TDRC(encoding=3, text=year))
            
            audio_file.save()
        except Exception as e:
            print(f"Error adding metadata to {file_path}: {e}")
