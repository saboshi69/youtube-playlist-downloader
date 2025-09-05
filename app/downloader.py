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

    def _validate_uploader(self, uploader_str: Optional[str], channel_title: Optional[str] = None) -> str:
        """FIXED: Validate uploader field and prevent duration/garbage values"""
        if not uploader_str or not isinstance(uploader_str, str):
            return channel_title or "Unknown Artist"
        
        uploader_str = uploader_str.strip()
        
        if not uploader_str:
            return channel_title or "Unknown Artist"

        # REJECT: Duration-like strings (e.g., "3:42", "12:34", "1:05")
        # This pattern matches: 1-2 digits, colon, exactly 2 digits
        if re.match(r'^\d{1,2}:\d{2}$', uploader_str):
            print(f"⚠️ [UPLOADER] Rejected duration pattern: '{uploader_str}' → using '{channel_title or 'Unknown Artist'}'")
            return channel_title or "Unknown Artist"
        
        # REJECT: Plain numbers (e.g., "123456")
        if uploader_str.isdigit():
            print(f"⚠️ [UPLOADER] Rejected numeric: '{uploader_str}' → using '{channel_title or 'Unknown Artist'}'")
            return channel_title or "Unknown Artist"
        
        # REJECT: Very short strings (likely garbage)
        if len(uploader_str) <= 1:
            print(f"⚠️ [UPLOADER] Rejected too short: '{uploader_str}' → using '{channel_title or 'Unknown Artist'}'")
            return channel_title or "Unknown Artist"
        
        # ACCEPT: Valid uploader name
        print(f"✅ [UPLOADER] Validated: '{uploader_str}'")
        return uploader_str


    def get_playlist_dual_source_complete(self, playlist_url: str) -> Dict:
        """COMPLETE DUAL-SOURCE WORKFLOW AS PER YOUR SPECIFICATIONS"""
        playlist_id = self._extract_playlist_id(playlist_url)
        print(f"🚀 [DUAL] Starting complete dual-source workflow for playlist: {playlist_url}")

        # STEP 1: ytmusicapi batch → grab the whole playlist
        print("🔍 [STEP 1] Fetching playlist with ytmusicapi...")
        ytmusic_tracks = {}
        ytmusic_result = {}
        
        try:
            ytmusic_result = self.get_playlist_info_batch(playlist_url)
            for track in ytmusic_result.get('entries', []):
                if track.get('id'):
                    ytmusic_tracks[track['id']] = track
            print(f"✅ [STEP 1] ytmusicapi found: {len(ytmusic_tracks)} tracks")
        except Exception as e:
            print(f"❌ [STEP 1] ytmusicapi failed: {e}")
            ytmusic_tracks = {}

        # STEP 3: yt-dlp batch → grab the playlist once more
        print("🔍 [STEP 3] Fetching playlist with yt-dlp...")
        ytdlp_tracks = {}
        ytdlp_result = {}
        
        try:
            ytdlp_result = self._get_playlist_with_ytdlp(playlist_url)
            for track in ytdlp_result.get('entries', []):
                if track.get('id'):
                    ytdlp_tracks[track['id']] = track
            print(f"✅ [STEP 3] yt-dlp found: {len(ytdlp_tracks)} tracks")
        except Exception as e:
            print(f"❌ [STEP 3] yt-dlp failed: {e}")
            ytdlp_tracks = {}

        # STEP 4: Compare both videoID → find which tracks ytmusicapi missed
        print("🔍 [STEP 4] Comparing video IDs to find missing tracks...")
        ytmusic_ids = set(ytmusic_tracks.keys())
        ytdlp_ids = set(ytdlp_tracks.keys())
        missing_from_ytmusic = ytdlp_ids - ytmusic_ids

        print(f"📊 [STEP 4] Comparison results:")
        print(f"   - ytmusicapi tracks: {len(ytmusic_ids)}")
        print(f"   - yt-dlp tracks: {len(ytdlp_ids)}")
        print(f"   - Missing from ytmusicapi: {len(missing_from_ytmusic)}")

        # STEP 5: For each missing one: Try ytmusicapi.get_song(id_from_ytdlp)
        enriched_tracks = {}
        failed_enrichment = []
        
        if missing_from_ytmusic:
            print(f"🔄 [STEP 5] Enriching {len(missing_from_ytmusic)} missing tracks with ytmusicapi.get_song...")
            
            for video_id in missing_from_ytmusic:
                print(f"   🎵 [STEP 5] Trying ytmusicapi.get_song for: {video_id}")
                
                try:
                    if self.ytmusic:
                        song_data = self.ytmusic.get_song(video_id)
                        if song_data and song_data.get('videoDetails'):
                            enriched_track = self._parse_song_data_complete(song_data, video_id)
                            enriched_tracks[video_id] = enriched_track
                            print(f"   ✅ [STEP 5] Enriched from get_song: {enriched_track.get('title', video_id)}")
                            continue
                except Exception as e:
                    print(f"   ⚠️ [STEP 5] get_song failed for {video_id}: {e}")
                
                # If ytmusicapi.get_song failed, add to failed list for Step 6
                failed_enrichment.append(video_id)

        # STEP 6: IF YTMUSICAPI still not found it, use ytdlp metadata
        if failed_enrichment:
            print(f"🔄 [STEP 6] Using yt-dlp metadata for {len(failed_enrichment)} remaining tracks...")
            
            for video_id in failed_enrichment:
                if video_id in ytdlp_tracks:
                    enriched_tracks[video_id] = ytdlp_tracks[video_id]
                    print(f"   ✅ [STEP 6] Using yt-dlp data for: {ytdlp_tracks[video_id].get('title', video_id)}")

        # Combine all tracks: ytmusicapi + enriched missing tracks
        all_tracks = {}
        all_tracks.update(ytmusic_tracks)  # Original ytmusicapi tracks
        all_tracks.update(enriched_tracks)  # Missing tracks (enriched or from yt-dlp)

        final_entries = list(all_tracks.values())
        
        print(f"🎯 [COMPLETE] Final result:")
        print(f"   - Original ytmusicapi tracks: {len(ytmusic_tracks)}")
        print(f"   - Enriched via get_song: {len([t for t in enriched_tracks.values() if t.get('source') == 'ytmusic_get_song'])}")
        print(f"   - Fallback yt-dlp tracks: {len([t for t in enriched_tracks.values() if t.get('source') == 'ytdlp_fallback'])}")
        print(f"   - TOTAL TRACKS: {len(final_entries)}")

        return {
            'title': ytmusic_result.get('title') or ytdlp_result.get('title', 'Unknown Playlist'),
            'entries': final_entries,
            'playlist_id': playlist_id,
            'stats': {
                'ytmusic_tracks': len(ytmusic_tracks),
                'ytdlp_tracks': len(ytdlp_tracks),
                'enriched_tracks': len(enriched_tracks),
                'total_tracks': len(final_entries)
            }
        }

    def _get_playlist_with_ytdlp(self, playlist_url: str) -> Dict:
        """Get playlist using yt-dlp (Step 3)"""
        print(f"🔍 [YT-DLP] Extracting playlist: {playlist_url}")
        
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
                    raise Exception("Invalid info returned from yt-dlp")

                entries_raw = info.get('entries', [])
                if not entries_raw:
                    raise Exception("No entries found in playlist")

                entries = []
                for entry in entries_raw:
                    if not isinstance(entry, dict) or not entry.get('id'):
                        continue

                    # Skip restricted content
                    availability = entry.get('availability', 'public')
                    if availability in ['private', 'subscriber_only', 'premium_only', 'needs_auth']:
                        continue

                    # FIXED: Validate uploader field
                    raw_uploader = entry.get('uploader', 'Unknown Artist')
                    channel_title = entry.get('channel', entry.get('uploader_id', None))
                    validated_uploader = self._validate_uploader(raw_uploader, channel_title)

                    video_entry = {
                        'id': entry.get('id'),
                        'title': entry.get('title') or f"Video {entry.get('id')}",
                        'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                        'availability': availability,
                        'duration': entry.get('duration'),
                        'uploader': validated_uploader,
                        'upload_date': entry.get('upload_date', ''),
                        'album': 'Unknown Album',
                        'artist': validated_uploader,
                        'thumbnail': entry.get('thumbnail'),
                        'year': None,
                        'source': 'ytdlp_fallback'
                    }
                    entries.append(video_entry)

                print(f"✅ [YT-DLP] Successfully extracted {len(entries)} videos")
                return {
                    'title': info.get('title', 'Unknown Playlist'),
                    'entries': entries
                }

        except Exception as e:
            print(f"❌ [YT-DLP] Extraction failed: {e}")
            return {'title': 'Unknown Playlist', 'entries': []}

    def _parse_song_data_complete(self, song_data: Dict, video_id: str) -> Dict:
        """FIXED: Parse ytmusicapi get_song result with proper uploader validation"""
        video_details = song_data.get('videoDetails', {})
        
        # Extract additional metadata if available
        microformat = song_data.get('microformat', {}).get('microformatDataRenderer', {})
        
        # FIXED: Validate uploader from multiple sources
        raw_uploader = video_details.get('author', 'Unknown Artist')
        channel_title = microformat.get('ownerChannelName') or video_details.get('channelId')
        validated_uploader = self._validate_uploader(raw_uploader, channel_title)
        
        return {
            'id': video_id,
            'title': video_details.get('title', 'Unknown Title'),
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'availability': 'public',
            'duration': int(video_details.get('lengthSeconds', 0)),
            'uploader': validated_uploader,
            'upload_date': microformat.get('uploadDate', ''),
            'album': 'Unknown Album',  # get_song doesn't provide album info
            'artist': validated_uploader,
            'thumbnail': self._extract_best_thumbnail(video_details.get('thumbnail', {})),
            'year': None,
            'source': 'ytmusic_get_song'
        }

    def _extract_best_thumbnail(self, thumbnail_data: Dict) -> Optional[str]:
        """Extract best quality thumbnail URL"""
        thumbnails = thumbnail_data.get('thumbnails', [])
        if thumbnails and isinstance(thumbnails, list):
            # Return highest quality thumbnail (last in list)
            return thumbnails[-1].get('url')
        return None

    # Keep existing methods for backward compatibility
    def get_playlist_info_batch(self, playlist_url: str) -> Dict:
        """ORIGINAL METHOD: Get entire playlist data with ytmusicapi only"""
        print(f"🔍 [YTMUSIC] Extracting playlist with ytmusicapi: {playlist_url}")
        
        if not self.ytmusic:
            raise Exception("❌ YTMusic API not available")

        try:
            playlist_id = self._extract_playlist_id(playlist_url)
            if not playlist_id:
                raise ValueError("Invalid playlist URL")

            playlist_info = self.ytmusic.get_playlist(playlist_id, limit=1000)
            
            if not playlist_info or not playlist_info.get('tracks'):
                raise Exception("No tracks found in playlist")

            print(f"✅ [YTMUSIC] Successfully fetched {len(playlist_info['tracks'])} tracks")

            entries = []
            for i, track in enumerate(playlist_info['tracks']):
                try:
                    if not track or not isinstance(track, dict) or not track.get('videoId'):
                        print(f"⚠️ [YTMUSIC] Skipping invalid track {i}")
                        continue

                    # FIXED: Extract and validate uploader
                    raw_uploader = self._extract_artist_name_safe(track)
                    validated_uploader = self._validate_uploader(raw_uploader, None)

                    video_entry = {
                        'id': track['videoId'],
                        'title': track.get('title', 'Unknown Title'),
                        'url': f"https://www.youtube.com/watch?v={track['videoId']}",
                        'availability': 'public',
                        'duration': track.get('duration_seconds', 0),
                        'uploader': validated_uploader,
                        'upload_date': '',
                        'album': self._extract_album_name_safe(track),
                        'artist': validated_uploader,
                        'thumbnail': self._extract_thumbnail_safe(track),
                        'year': self._extract_year_safe(track),
                        'source': 'ytmusic_playlist'
                    }
                    entries.append(video_entry)
                except Exception as track_error:
                    print(f"⚠️ [YTMUSIC] Error processing track {i}: {track_error}")
                    continue

            return {
                'title': playlist_info.get('title', 'Unknown Playlist'),
                'entries': entries,
                'playlist_id': playlist_id
            }

        except Exception as e:
            print(f"❌ [YTMUSIC] Playlist fetch failed: {e}")
            raise Exception(f"Failed to fetch playlist: {e}")

    # Alias for the complete dual-source method
    def get_playlist_dual_source(self, playlist_url: str) -> Dict:
        """Main method that implements your complete dual-source workflow"""
        return self.get_playlist_dual_source_complete(playlist_url)

    def download_video(self, video_url: str, video_id: str, playlist_id: str = None) -> Optional[Dict]:
        """FIXED: Download video using database metadata for proper naming and tagging"""
        print(f"🎵 [DOWNLOAD] Starting download for {video_id}")

        # Get ALL metadata from database (including uploader and parsed metadata.album)
        db_info = self._get_complete_database_info(video_id)
        if not db_info:
            print(f"❌ [DOWNLOAD] No database info for {video_id} - skipping!")
            return None

        # FIXED: Use database values for proper naming and tagging
        title = db_info.get('title', f'video_{video_id}')
        uploader = db_info.get('uploader', 'Unknown Artist')  # FIX #3: Use uploader from DB
        album_name = db_info.get('album', 'Unknown Album')    # FIX #2: Use album from metadata
        year = db_info.get('year')
        thumbnail_url = db_info.get('thumbnail')

        print(f"✅ [DOWNLOAD] Using DB metadata: {title} | {uploader} | {album_name}")
        
        return self._perform_download_with_correct_metadata(video_url, video_id, title, uploader, album_name, year, thumbnail_url)

    def _get_complete_database_info(self, video_id: str) -> Optional[Dict]:
        """FIXED: Get complete info from database including uploader and parsed album"""
        try:
            with sqlite3.connect(self.config.DATABASE_PATH) as conn:
                cursor = conn.execute('''
                    SELECT title, uploader, metadata 
                    FROM videos 
                    WHERE video_id = ?
                ''', (video_id,))
                row = cursor.fetchone()
                
                if row:
                    title, uploader, metadata_json = row
                    
                    # Parse metadata JSON
                    metadata = {}
                    if metadata_json:
                        try:
                            metadata = json.loads(metadata_json)
                        except:
                            metadata = {}
                    
                    # FIXED: Extract album from metadata JSON + validate uploader
                    album = metadata.get('album', 'Unknown Album')
                    validated_uploader = self._validate_uploader(uploader, None)
                    
                    return {
                        'title': title or 'Unknown Title',
                        'uploader': validated_uploader,  # FIX #3 + Validation
                        'album': album,                  # FIX #2
                        'year': metadata.get('year'),
                        'thumbnail': metadata.get('thumbnail')
                    }
                    
        except Exception as e:
            print(f"❌ Database error for {video_id}: {e}")
        return None

    def _perform_download_with_correct_metadata(self, video_url: str, video_id: str, title: str, uploader: str, album: str, year: Optional[str], thumbnail_url: Optional[str]) -> Optional[Dict]:
        """FIXED: Perform download with correct filename and metadata"""
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

            # FIXED: Create filename using database title (FIX #1)
            clean_title = self._sanitize_filename(title)
            if clean_title == 'unknown' or clean_title.startswith('video_'):
                clean_title = self._sanitize_filename(f"{uploader}_{title}"[:50])

            # Download with correct filename
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

            # Find downloaded file
            actual_file_path = self._find_downloaded_file(clean_title, f"video_{video_id}")
            if not actual_file_path or not os.path.exists(actual_file_path):
                print(f"❌ File not found after download for {video_id}")
                return None

            file_size = os.path.getsize(actual_file_path)
            file_hash = self._calculate_file_hash(actual_file_path)

            # FIXED: Add proper metadata to MP3 using database values
            combined_info = {
                'title': title,           # From database
                'artist': uploader,       # From database uploader field (validated)
                'album': album,           # From metadata.album in database  
                'year': year,
                'thumbnail': thumbnail_url,
                'upload_date': tech_info.get('upload_date', ''),
                'duration': tech_info.get('duration', 0),
                'description': tech_info.get('description', ''),
                'view_count': tech_info.get('view_count', 0)
            }

            self._add_mp3_metadata_fixed(actual_file_path, combined_info)
            print(f"✅ Downloaded: {title} by {uploader} [{album}] ({file_size} bytes)")

            # Delay if configured
            if self.config.DOWNLOAD_DELAY_ENABLED:
                wait_time = random.uniform(self.config.DOWNLOAD_DELAY_MIN, self.config.DOWNLOAD_DELAY_MAX)
                print(f"⏰ Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)

            return {
                'video_id': video_id,
                'title': title,
                'uploader': uploader,
                'duration': combined_info.get('duration', 0),
                'upload_date': combined_info.get('upload_date', ''),
                'file_path': actual_file_path,
                'file_hash': file_hash,
                'file_size': file_size,
                'status': 'downloaded',
                'metadata': {
                    'description': combined_info.get('description', ''),
                    'view_count': combined_info.get('view_count', 0),
                    'album': album,
                    'year': year,
                    'localization': 'HK_dual_source_complete_fixed'
                }
            }

        except Exception as e:
            print(f"❌ Download failed for {video_id}: {e}")
            return None

    def _add_mp3_metadata_fixed(self, file_path: str, info: Dict):
        """FIXED: Add proper MP3 metadata using database values"""
        try:
            if not file_path or not file_path.lower().endswith('.mp3'):
                return

            audio_file = MP3(file_path, ID3=ID3)
            if audio_file.tags is None:
                audio_file.add_tags()

            # FIXED: Use correct values from database
            title = info.get('title', 'Unknown Title')
            artist = info.get('artist', 'Unknown Artist')  # This is uploader from database (validated)
            album = info.get('album', 'Unknown Album')     # This is album from metadata
            year = info.get('year')

            print(f"🏷️ [METADATA] Setting: Title='{title}', Artist='{artist}', Album='{album}', Year='{year}'")

            # Set ID3 tags with database values
            audio_file.tags.add(TIT2(encoding=3, text=title))
            audio_file.tags.add(TPE1(encoding=3, text=artist))
            audio_file.tags.add(TALB(encoding=3, text=album))

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
            print(f"✅ [METADATA] Successfully set: {title} | {artist} | {album} | {year}")

        except Exception as e:
            print(f"❌ Metadata error: {e}")

    # Helper methods with uploader validation
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
