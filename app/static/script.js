class PlaylistDownloader {
    constructor() {
        this.initializeEventListeners();
        this.loadData();
        this.startStatusUpdates();
    }

    initializeEventListeners() {
        document.getElementById('playlist-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.addPlaylist();
        });

        document.getElementById('check-now').addEventListener('click', () => {
            this.triggerCheck();
        });
    }

    async loadData() {
        await this.loadStatus();
        await this.loadPlaylists();
        await this.loadDownloads();
    }

    async loadStatus() {
        try {
            const response = await fetch('/api/status');
            const status = await response.json();
            
            document.getElementById('status').textContent = status.monitoring ? 'Running' : 'Stopped';
            document.getElementById('status').className = status.monitoring ? 'status-value active' : 'status-value';
            document.getElementById('playlist-count').textContent = status.total_playlists;
            document.getElementById('download-count').textContent = status.total_downloads;
            document.getElementById('current-activity').textContent = status.current_activity;
            
            if (status.last_check) {
                const lastCheck = new Date(status.last_check).toLocaleString();
                document.getElementById('last-check').textContent = lastCheck;
            }
        } catch (error) {
            console.error('Error loading status:', error);
        }
    }

    async loadPlaylists() {
        try {
            const response = await fetch('/api/playlists');
            const playlists = await response.json();
            
            const container = document.getElementById('playlists-list');
            
            if (playlists.length === 0) {
                container.innerHTML = '<div class="loading">No playlists added yet</div>';
                return;
            }
            
            container.innerHTML = playlists.map(playlist => `
                <div class="playlist-item">
                    <div class="playlist-info">
                        <h4>${this.escapeHtml(playlist.name || 'Unnamed Playlist')}</h4>
                        <div class="url">${this.escapeHtml(playlist.url)}</div>
                        <div class="playlist-stats">
                            <span class="stat">Videos: ${playlist.video_count || 0}</span>
                            <span class="stat">Downloaded: ${playlist.downloaded_count || 0}</span>
                            <span class="stat">Added: ${new Date(playlist.last_checked || Date.now()).toLocaleDateString()}</span>
                        </div>
                    </div>
                    <button class="btn btn-danger" onclick="app.removePlaylist(${playlist.id})">Remove</button>
                </div>
            `).join('');
        } catch (error) {
            console.error('Error loading playlists:', error);
        }
    }

    async loadDownloads() {
        try {
            const response = await fetch('/api/downloads');
            const downloads = await response.json();
            
            const container = document.getElementById('downloads-list');
            
            if (downloads.length === 0) {
                container.innerHTML = '<div class="loading">No downloads yet</div>';
                return;
            }
            
            container.innerHTML = downloads.map(download => `
                <div class="download-item">
                    <h4>${this.escapeHtml(download.title)}</h4>
                    <div class="download-meta">
                        ${this.escapeHtml(download.uploader)} • 
                        ${this.escapeHtml(download.playlist_name)} • 
                        ${new Date(download.download_date).toLocaleString()}
                    </div>
                </div>
            `).join('');
        } catch (error) {
            console.error('Error loading downloads:', error);
        }
    }

    async addPlaylist() {
        const url = document.getElementById('playlist-url').value;
        const name = document.getElementById('playlist-name').value;

        if (!url) return;

        try {
            const response = await fetch('/api/playlists', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ url, name }),
            });

            const result = await response.json();

            if (response.ok) {
                this.showMessage(result.message, 'success');
                document.getElementById('playlist-form').reset();
                await this.loadData();
            } else {
                this.showMessage(result.detail || 'Error adding playlist', 'error');
            }
        } catch (error) {
            this.showMessage('Network error: ' + error.message, 'error');
        }
    }

    async removePlaylist(playlistId) {
        if (!confirm('Are you sure you want to remove this playlist from monitoring?')) {
            return;
        }

        try {
            const response = await fetch(`/api/playlists/${playlistId}`, {
                method: 'DELETE',
            });

            const result = await response.json();

            if (response.ok) {
                this.showMessage(result.message, 'success');
                await this.loadData();
            } else {
                this.showMessage(result.detail || 'Error removing playlist', 'error');
            }
        } catch (error) {
            this.showMessage('Network error: ' + error.message, 'error');
        }
    }

    async triggerCheck() {
        try {
            const response = await fetch('/api/check-now', {
                method: 'POST',
            });

            const result = await response.json();

            if (response.ok) {
                this.showMessage(result.message, 'success');
                setTimeout(() => this.loadStatus(), 1000);
            } else {
                this.showMessage(result.detail || 'Error triggering check', 'error');
            }
        } catch (error) {
            this.showMessage('Network error: ' + error.message, 'error');
        }
    }

    startStatusUpdates() {
        // Update status every 30 seconds
        setInterval(() => {
            this.loadStatus();
        }, 30000);

        // Update downloads every 2 minutes
        setInterval(() => {
            this.loadDownloads();
        }, 120000);
    }

    showMessage(message, type) {
        const existingMessage = document.querySelector('.success-message, .error-message');
        if (existingMessage) {
            existingMessage.remove();
        }

        const messageDiv = document.createElement('div');
        messageDiv.className = `${type}-message`;
        messageDiv.textContent = message;

        const form = document.getElementById('playlist-form');
        form.parentNode.insertBefore(messageDiv, form);

        setTimeout(() => {
            if (messageDiv.parentNode) {
                messageDiv.remove();
            }
        }, 5000);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize the app
const app = new PlaylistDownloader();
