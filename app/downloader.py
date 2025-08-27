import os
import yt_dlp
import hashlib
from typing import Dict, Optional
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC

class YouTubeDownloader:
    def __init__(self, download_dir: str, audio_quality: str = '320'):
        self.download_dir = download_dir
        self.audio_quality = audio_quality
        os.makedirs(download_dir, exist_ok=True)
        
        # Base configuration for yt-dlp
        self.base_ydl_opts = {
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': self.audio_quality,
            'embed_metadata': True,
            'writeinfojson': False,
            'ignoreerrors': True,
            'no_warnings': True,
            'quiet': True,
            # Fix for sign-in issues
            'cookiefile': None,
            'age_limit': None,
            'skip_download': False,
            # User agent to avoid detection
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            # Additional headers
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            },
            # Bypass geo-blocking and age restrictions
            'geo_bypass': True,
            'geo_bypass_country': 'US',
            # Additional bypass options
            'extractor_retries': 3,
            'fragment_retries': 3,
            'retry_sleep_functions': {
                'http': lambda n: min(2 ** n, 60),
                'fragment': lambda n: min(2 ** n, 60),
                'extractor': lambda n: min(2 ** n, 60),
            }
        }
    
    def get_playlist_info(self, playlist_url: str) -> Dict:
        """Extract playlist information without downloading"""
        ydl_opts = {
            **self.base_ydl_opts,
            'extract_flat': True,
            'quiet': True,
            'no_warnings': True,
        }
        
        # Try multiple extraction methods
        extraction_methods = [
            # Method 1: Standard extraction
            ydl_opts,
            # Method 2: With different format selector
            {**ydl_opts, 'format': 'best[height<=720]'},
            # Method 3: With bypass options
            {**ydl_opts, 'extractor_args': {'youtube': {'skip': ['dash', 'hls']}}},
            # Method 4: Minimal options
            {
                'extract_flat': True,
                'quiet': True,
                'no_warnings': True,
                'ignoreerrors': True,
            }
        ]
        
        for i, opts in enumerate(extraction_methods):
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    print(f"Trying extraction method {i+1} for playlist...")
                    info = ydl.extract_info(playlist_url, download=False)
                    
                    if info and info.get('entries'):
                        entries = []
                        for entry in info.get('entries', []):
                            if entry and entry.get('id'):
                                # Skip private or unavailable videos
                                availability = entry.get('availability', 'public')
                                if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                                    print(f"Skipping restricted video: {entry.get('title', entry.get('id'))} ({availability})")
                                    continue
                                    
                                entries.append({
                                    'id': entry.get('id'),
                                    'title': entry.get('title') or f"Video {entry.get('id')}",
                                    'url': f"https://youtube.com/watch?v={entry.get('id')}",
                                    'availability': availability,
                                    'duration': entry.get('duration'),
                                    'uploader': entry.get('uploader'),
                                    'upload_date': entry.get('upload_date')
                                })
                        
                        print(f"Successfully extracted {len(entries)} videos from playlist")
                        return {
                            'title': info.get('title', 'Unknown Playlist'),
                            'entries': entries,
                            'id': info.get('id'),
                            'uploader': info.get('uploader'),
                            'description': info.get('description')
                        }
                        
            except Exception as e:
                print(f"Extraction method {i+1} failed: {e}")
                if 'sign in' in str(e).lower():
                    print("Playlist requires authentication - skipping")
                    break
                continue
        
        print("All extraction methods failed")
        return {'title': None, 'entries': []}
    
    def download_video(self, video_url: str, video_id: str) -> Optional[Dict]:
        """Download a single video and return metadata"""
        safe_title = f"video_{video_id}"
        file_path = os.path.join(self.download_dir, f"{safe_title}.mp3")
        
        # Multiple download strategies with increasing fallback
        download_strategies = [
            # Strategy 1: High quality audio
            {
                **self.base_ydl_opts,
                'format': 'bestaudio[ext=m4a]/bestaudio/best',
                'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': self.audio_quality,
                }]
            },
            # Strategy 2: Standard quality
            {
                **self.base_ydl_opts,
                'format': 'bestaudio[abr<=192]/best[height<=720]',
                'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
                'audioquality': '192',
            },
            # Strategy 3: Lower quality fallback
            {
                **self.base_ydl_opts,
                'format': 'bestaudio[abr<=128]/best[height<=480]',
                'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
                'audioquality': '128',
            },
            # Strategy 4: Any available format
            {
                **self.base_ydl_opts,
                'format': 'worst/worstaudio',
                'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
                'audioquality': '96',
            },
            # Strategy 5: Minimal options (last resort)
            {
                'format': 'bestaudio/best',
                'outtmpl': file_path.replace('.mp3', '.%(ext)s'),
                'extractaudio': True,
                'audioformat': 'mp3',
                'ignoreerrors': True,
                'no_warnings': True,
                'quiet': True,
            }
        ]
        
        last_error = None
        
        for i, ydl_opts in enumerate(download_strategies):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    print(f"Trying download strategy {i+1} for {video_id}...")
                    
                    # Extract info first to check availability
                    try:
                        info = ydl.extract_info(video_url, download=False)
                    except Exception as extract_error:
                        print(f"Info extraction failed: {extract_error}")
                        last_error = extract_error
                        
                        # Handle specific errors
                        error_str = str(extract_error).lower()
                        if any(keyword in error_str for keyword in ['sign in', 'login', 'authenticate', 'private']):
                            return {
                                'video_id': video_id,
                                'title': f'Sign-in Required: {video_id}',
                                'uploader': 'Unknown',
                                'duration': 0,
                                'upload_date': '',
                                'file_path': None,
                                'file_hash': None,
                                'file_size': 0,
                                'status': 'restricted',
                                'metadata': {'error': 'Requires sign-in', 'availability': 'requires_auth'}
                            }
                        
                        # For other errors, try next strategy
                        continue
                    
                    if not info:
                        print(f"No info available for {video_id}")
                        continue
                    
                    # Check if video is available
                    availability = info.get('availability', 'public')
                    if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                        print(f"Video {video_id} is not publicly available: {availability}")
                        return {
                            'video_id': video_id,
                            'title': info.get('title', f'Restricted: {video_id}'),
                            'uploader': info.get('uploader', 'Unknown'),
                            'duration': info.get('duration', 0),
                            'upload_date': info.get('upload_date', ''),
                            'file_path': None,
                            'file_hash': None,
                            'file_size': 0,
                            'status': 'restricted',
                            'metadata': {
                                'availability': availability,
                                'description': info.get('description', ''),
                                'view_count': info.get('view_count', 0)
                            }
                        }
                    
                    # Check if video is too long (over 30 minutes)
                    duration = info.get('duration', 0)
                    if duration and duration > 1800:  # 30 minutes
                        print(f"Video {video_id} is too long ({duration//60} minutes), skipping")
                        return {
                            'video_id': video_id,
                            'title': info.get('title', f'Too Long: {video_id}'),
                            'uploader': info.get('uploader', 'Unknown'),
                            'duration': duration,
                            'upload_date': info.get('upload_date', ''),
                            'file_path': None,
                            'file_hash': None,
                            'file_size': 0,
                            'status': 'skipped',
                            'metadata': {'reason': 'Too long', 'duration_minutes': duration//60}
                        }
                    
                    # Update filename with actual title
                    title = info.get('title', f'video_{video_id}')
                    safe_title = self._sanitize_filename(title)
                    file_path = os.path.join(self.download_dir, f"{safe_title}.mp3")
                    ydl_opts['outtmpl'] = file_path.replace('.mp3', '.%(ext)s')
                    
                    # Download
                    try:
                        ydl.download([video_url])
                    except Exception as download_error:
                        print(f"Download failed: {download_error}")
                        last_error = download_error
                        
                        # Handle specific download errors
                        error_str = str(download_error).lower()
                        if any(keyword in error_str for keyword in ['sign in', 'login', 'authenticate']):
                            return {
                                'video_id': video_id,
                                'title': title,
                                'uploader': info.get('uploader', 'Unknown'),
                                'duration': info.get('duration', 0),
                                'upload_date': info.get('upload_date', ''),
                                'file_path': None,
                                'file_hash': None,
                                'file_size': 0,
                                'status': 'restricted',
                                'metadata': {'error': 'Requires sign-in', 'availability': 'requires_auth'}
                            }
                        
                        # Try next strategy
                        continue
                    
                    # Verify download success and find the actual file
                    actual_file_path = self._find_downloaded_file(file_path, safe_title)
                    
                    if actual_file_path and os.path.exists(actual_file_path):
                        # Calculate file hash and size
                        file_hash = self._calculate_file_hash(actual_file_path)
                        file_size = os.path.getsize(actual_file_path)
                        
                        # Add metadata to MP3 file
                        self._add_mp3_metadata(actual_file_path, info)
                        
                        print(f"Successfully downloaded: {title}")
                        return {
                            'video_id': video_id,
                            'title': title,
                            'uploader': info.get('uploader', 'Unknown'),
                            'duration': info.get('duration', 0),
                            'upload_date': info.get('upload_date', ''),
                            'file_path': actual_file_path,
                            'file_hash': file_hash,
                            'file_size': file_size,
                            'status': 'downloaded',
                            'metadata': {
                                'description': info.get('description', ''),
                                'view_count': info.get('view_count', 0),
                                'like_count': info.get('like_count', 0),
                                'channel': info.get('channel', ''),
                                'tags': info.get('tags', []),
                                'availability': availability
                            }
                        }
                    else:
                        print(f"Download completed but file not found: {file_path}")
                        last_error = f"File not found after download: {file_path}"
                        continue
                        
            except Exception as e:
                print(f"Strategy {i+1} failed for {video_id}: {e}")
                last_error = e
                continue
        
        # All strategies failed - return failed status with last error
        print(f"All download strategies failed for {video_id}. Last error: {last_error}")
        
        return {
            'video_id': video_id,
            'title': f'Failed: {video_id}',
            'uploader': 'Unknown',
            'duration': 0,
            'upload_date': '',
            'file_path': None,
            'file_hash': None,
            'file_size': 0,
            'status': 'failed',
            'metadata': {'error': str(last_error) if last_error else 'Unknown error'}
        }
    
    def _find_downloaded_file(self, expected_path: str, safe_title: str) -> Optional[str]:
        """Find the actual downloaded file (yt-dlp sometimes changes extensions)"""
        possible_paths = [
            expected_path,
            expected_path.replace('.mp3', '.m4a'),
            expected_path.replace('.mp3', '.webm'),
            expected_path.replace('.mp3', '.opus'),
            expected_path.replace('.mp3', '.mp4'),
            os.path.join(self.download_dir, f"{safe_title}.mp3"),
            os.path.join(self.download_dir, f"{safe_title}.m4a"),
            os.path.join(self.download_dir, f"{safe_title}.webm"),
            os.path.join(self.download_dir, f"{safe_title}.opus"),
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                # If it's not mp3, try to rename it
                if not path.endswith('.mp3'):
                    new_path = path.rsplit('.', 1)[0] + '.mp3'
                    try:
                        os.rename(path, new_path)
                        return new_path
                    except:
                        return path
                return path
        
        # If still not found, search directory for files with similar names
        try:
            for file in os.listdir(self.download_dir):
                if safe_title in file and file.endswith(('.mp3', '.m4a', '.webm', '.opus')):
                    return os.path.join(self.download_dir, file)
        except:
            pass
        
        return None
    
    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove multiple spaces and trim
        filename = ' '.join(filename.split())
        
        # Limit length and remove trailing dots/spaces
        filename = filename[:200].strip('. ')
        
        # Ensure filename is not empty
        if not filename:
            filename = 'unknown'
        
        return filename
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file for duplicate detection"""
        if not os.path.exists(file_path):
            return None
        
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            print(f"Error calculating hash for {file_path}: {e}")
            return None
    
    def _add_mp3_metadata(self, file_path: str, info: Dict):
        """Add metadata to MP3 file"""
        try:
            # Only try to add metadata to MP3 files
            if not file_path.lower().endswith('.mp3'):
                return
            
            audio_file = MP3(file_path, ID3=ID3)
            
            # Add ID3 tag if it doesn't exist
            try:
                if audio_file.tags is None:
                    audio_file.add_tags()
            except:
                return
            
            # Add metadata safely
            try:
                title = info.get('title', '')
                if title:
                    audio_file.tags.add(TIT2(encoding=3, text=title))
            except:
                pass
            
            try:
                uploader = info.get('uploader', '') or info.get('channel', '')
                if uploader:
                    audio_file.tags.add(TPE1(encoding=3, text=uploader))
            except:
                pass
            
            try:
                audio_file.tags.add(TALB(encoding=3, text='YouTube Download'))
            except:
                pass
            
            try:
                upload_date = info.get('upload_date', '')
                if upload_date and len(upload_date) >= 4:
                    year = upload_date[:4]
                    audio_file.tags.add(TDRC(encoding=3, text=year))
            except:
                pass
            
            # Save metadata
            try:
                audio_file.save()
                print(f"Added metadata to: {os.path.basename(file_path)}")
            except Exception as save_error:
                print(f"Error saving metadata to {file_path}: {save_error}")
                
        except Exception as e:
            print(f"Error adding metadata to {file_path}: {e}")
    
    def get_video_info(self, video_url: str) -> Dict:
        """Get video information without downloading"""
        ydl_opts = {
            **self.base_ydl_opts,
            'quiet': True,
            'no_warnings': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                
                if info:
                    return {
                        'id': info.get('id'),
                        'title': info.get('title'),
                        'uploader': info.get('uploader'),
                        'duration': info.get('duration'),
                        'upload_date': info.get('upload_date'),
                        'view_count': info.get('view_count'),
                        'like_count': info.get('like_count'),
                        'description': info.get('description'),
                        'availability': info.get('availability', 'public'),
                        'tags': info.get('tags', [])
                    }
        except Exception as e:
            print(f"Error getting video info: {e}")
        
        return {}
    
    def cleanup_partial_downloads(self):
        """Clean up partial downloads and temporary files"""
        try:
            for file in os.listdir(self.download_dir):
                file_path = os.path.join(self.download_dir, file)
                
                # Remove temporary files
                if file.endswith(('.part', '.tmp', '.temp')):
                    try:
                        os.remove(file_path)
                        print(f"Removed temporary file: {file}")
                    except:
                        pass
                
                # Remove very small files (likely failed downloads)
                elif file.endswith(('.mp3', '.m4a', '.webm', '.opus')):
                    try:
                        if os.path.getsize(file_path) < 1024:  # Less than 1KB
                            os.remove(file_path)
                            print(f"Removed incomplete file: {file}")
                    except:
                        pass
        except Exception as e:
            print(f"Error during cleanup: {e}")
    
    def get_download_stats(self) -> Dict:
        """Get statistics about downloaded files"""
        try:
            files = os.listdir(self.download_dir)
            audio_files = [f for f in files if f.endswith(('.mp3', '.m4a', '.webm', '.opus'))]
            
            total_size = 0
            for file in audio_files:
                try:
                    total_size += os.path.getsize(os.path.join(self.download_dir, file))
                except:
                    pass
            
            return {
                'total_files': len(audio_files),
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'download_dir': self.download_dir
            }
        except Exception as e:
            print(f"Error getting download stats: {e}")
            return {'total_files': 0, 'total_size_bytes': 0, 'total_size_mb': 0}
