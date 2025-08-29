
# YouTube Playlist Downloader

A self-hosted service to monitor and download songs from YouTube Music playlists. Ideal for automated batch downloads of your favorite playlists in high-quality audio (MP3).

---

## Features

- Monitors YouTube Music playlists for new songs
- Downloads songs automatically with configurable audio quality
- Avoids duplicate downloads using file hash checking
- Embeds metadata and album art into MP3 files
- Supports manual and scheduled playlist scans
- Configurable delay between downloads to avoid rate limits
- REST API and web interface for easy management

---

## Setup

### Requirements

- Docker / Docker Compose
- Python 3.10+ (if running outside container)
- FFmpeg installed (for audio extraction)

### Running with Docker

```bash
docker-compose up -d --build
```

### Environment Variables

| Variable             | Default                   | Description                               |
|----------------------|---------------------------|-------------------------------------------|
| `DATABASE_PATH`      | `/app/data/downloads.db`  | Path to SQLite database                   |
| `DOWNLOAD_DIR`       | `/downloads`              | Directory to save downloaded audio        |
| `CHECK_INTERVAL`     | `3600` (seconds)          | Interval between automatic playlist scans |
| `DOWNLOAD_DELAY_ENABLED` | `true`                   | Enable delay between downloads (true/false) |
| `DOWNLOAD_DELAY_MIN` | `60` (seconds)            | Minimum delay duration                     |
| `DOWNLOAD_DELAY_MAX` | `120` (seconds)           | Maximum delay duration                     |


---

## API Endpoints

- `GET /` - Web interface
- `GET /api/status` - Current status and stats
- `GET /api/playlists` - List monitored playlists
- `POST /api/playlists` - Add new playlist
- `DELETE /api/playlists/{id}` - Remove playlist
- `POST /api/check-now` - Trigger immediate playlist check
- `GET /api/downloads` - Recent downloads

---

## Usage

1. Add your playlists via the web UI or API.
2. The system will automatically download new songs.
3. Audio files are saved under your configured download directory.
4. Use the API for advanced management.

---

## Configuration

All configuration is done through environment variables or the `config.py` file.

---

## Known Issues

- Some videos may require authentication and can't be downloaded.
- Occasionally, YouTube's anti-bot measures may cause delays.

---

## License

MIT License

---

## Contributing

Feel free to open issues or submit pull requests.
