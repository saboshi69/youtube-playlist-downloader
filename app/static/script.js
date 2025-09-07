class PlaylistDownloader {
    constructor() {
        this.initializeEventListeners();
        this.loadData();
        this.startStatusUpdates();
        this.isProcessing = false;
    }

    initializeEventListeners() {
        // Add null checks for safety
        const playlistForm = document.getElementById('playlist-form');
        const checkNowBtn = document.getElementById('check-now');
        const validateBtn = document.getElementById('validate-files');

        if (playlistForm) {
            playlistForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.addPlaylist();
            });
        }

        if (checkNowBtn) {
            checkNowBtn.addEventListener('click', () => {
                this.triggerCheck();
            });
        }

        // NEW: Validate files button
        if (validateBtn) {
            validateBtn.addEventListener('click', () => {
                this.validateDownloadedFiles();
            });
        }
    }

    // NEW: Method to validate downloaded files
    async validateDownloadedFiles() {
        const btn = document.getElementById('validate-files');
        const originalText = btn.textContent;
        
        btn.disabled = true;
        btn.textContent = 'Validating...';
        
        try {
            const response = await fetch('/api/validate-downloads', {
                method: 'POST'
            });
            const result = await response.json();
            
            if (result.success) {
                this.showMessage(
                    `✅ Validation complete! Fixed ${result.fixed_count} missing files, ${result.valid_count} files are valid.`, 
                    'success'
                );
                // Refresh downloads list
                await this.loadDownloads();
            } else {
                this.showMessage(`❌ Validation failed: ${result.message}`, 'error');
            }
        } catch (error) {
            this.showMessage(`❌ Validation error: ${error.message}`, 'error');
        } finally {
            btn.disabled = false;
            btn.textContent = originalText;
        }
    }

    async loadPlaylists() {
        try {
            const response = await fetch('/api/playlists');
            
            if (!response.ok) {
                console.error('Failed to fetch playlists, status:', response.status);
                const container = document.getElementById('playlists-list');
                container.innerHTML = '<p class="error">Failed to load playlists</p>';
                return;
            }
            
            const playlists = await response.json();
            console.log('Loaded playlists:', playlists); // Debug log
            
            const container = document.getElementById('playlists-list');
            container.innerHTML = '';

            if (playlists.length === 0) {
                container.innerHTML = '<p>No playlists added yet.</p>';
                return;
            }

            playlists.forEach(playlist => {
                const div = document.createElement('div');
                div.className = 'playlist-item';

                const info = document.createElement('div');
                info.className = 'playlist-info';

                const title = document.createElement('h4');
                title.textContent = playlist.name || 'Untitled Playlist';

                const url = document.createElement('p');
                url.className = 'url';
                url.textContent = playlist.url;

                const stats = document.createElement('div');
                stats.className = 'playlist-stats';

                // Get status counts for each playlist
                this.getPlaylistStats(playlist.id).then(counts => {
                    const countDownloaded = document.createElement('span');
                    countDownloaded.className = 'stat';
                    countDownloaded.textContent = `Downloaded: ${counts.downloaded || 0}`;

                    const countPending = document.createElement('span');
                    countPending.className = 'stat';
                    countPending.textContent = `Pending: ${counts.pending || 0}`;

                    const countTotal = document.createElement('span');
                    countTotal.className = 'stat';
                    countTotal.textContent = `Total: ${counts.total || 0}`;

                    stats.appendChild(countDownloaded);
                    stats.appendChild(countPending);
                    stats.appendChild(countTotal);
                });

                info.appendChild(title);
                info.appendChild(url);
                info.appendChild(stats);
                div.appendChild(info);
                container.appendChild(div);
            });

        } catch (error) {
            console.error('Error loading playlists:', error);
            const container = document.getElementById('playlists-list');
            container.innerHTML = '<p class="error">Error loading playlists</p>';
        }
    }

    async getPlaylistStats(playlistId) {
        try {
            const response = await fetch(`/api/playlists/${playlistId}/stats`);
            return await response.json();
        } catch (error) {
            console.error('Error loading playlist stats:', error);
            return { downloaded: 0, pending: 0, total: 0 };
        }
    }

    async loadDownloads() {
        try {
            const response = await fetch('/api/downloads');
            const downloads = await response.json();

            const container = document.getElementById('downloads-list');
            container.innerHTML = '';

            if (downloads.length === 0) {
                container.innerHTML = '<p class="loading">No downloaded videos yet.</p>';
                return;
            }

            downloads.slice(0, 10).forEach(video => { // Show only latest 10
                const div = document.createElement('div');
                div.className = 'download-item';

                const title = document.createElement('h4');
                title.textContent = video.title || 'Unknown Title';

                const info = document.createElement('p');
                info.className = 'download-meta';
                const duration = video.duration ? `${Math.floor(video.duration / 60)}:${(video.duration % 60).toString().padStart(2, '0')}` : 'Unknown duration';
                info.textContent = `${video.uploader || 'Unknown'} - ${duration} - ${new Date(video.download_date).toLocaleString()}`;

                div.appendChild(title);
                div.appendChild(info);
                container.appendChild(div);
            });
        } catch (error) {
            console.error('Error loading downloads:', error);
        }
    }

    startStatusUpdates() {
        // Initial load
        this.loadStatus();
        
        // Adaptive polling based on activity
        setInterval(() => {
            this.loadStatus();
            
            // If processing, also refresh playlists and downloads more frequently
            if (this.isProcessing) {
                this.loadPlaylists();
                this.loadDownloads();
            }
        }, this.isProcessing ? 5000 : 10000); // 5s when busy, 10s when idle
    }

    async triggerCheck() {
        const btn = document.getElementById('check-now');
        const originalText = btn.textContent;
        
        btn.disabled = true;
        btn.textContent = 'Starting Check...';

        try {
            const response = await fetch('/api/check-now', { method: 'POST' });
            const data = await response.json();

            if (data.success) {
                this.showMessage('Playlist check started in background! Watch the status for progress.', 'success');
                // Immediately update activity status
                document.getElementById('current-activity').textContent = 'Manual check in progress...';
                this.isProcessing = true;
            } else {
                this.showMessage(data.message || 'Check already in progress', 'error');
            }
        } catch (error) {
            this.showMessage('Failed to start check: ' + error.message, 'error');
        } finally {
            btn.disabled = false;
            btn.textContent = originalText;
        }
    }

    async addPlaylist() {
        const form = document.getElementById('playlist-form');
        const formData = new FormData(form);
        const submitButton = form.querySelector('button[type="submit"]');
        const originalText = submitButton.textContent;

        // DEBUG: Log all form data to see what's actually there
        console.log('=== FormData Debug ===');
        for (let [key, value] of formData.entries()) {
            console.log(`${key}: ${value}`);
        }

        // Validate URL
        const url = formData.get('url');
        console.log('URL value:', url);
        
        if (!url || !url.includes('list=')) {
            this.showMessage('Please enter a valid YouTube playlist URL (must contain "list=")', 'error');
            return;
        }

        submitButton.disabled = true;
        submitButton.textContent = 'Adding Playlist...';

        try {
            const response = await fetch('/api/playlists', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    url: formData.get('url'),
                    name: formData.get('name') || 'Untitled Playlist'
                })
            });

            const result = await response.json();

            if (response.ok) {
                this.showMessage(`✅ Playlist "${result.playlist_name}" added! Processing ${result.total_videos} videos in background.`, 'success');
                form.reset();
                
                // Update UI
                document.getElementById('current-activity').textContent = `Processing new playlist: ${result.playlist_name}`;
                this.isProcessing = true;
                
                // Reload playlists to show the new one
                setTimeout(() => {
                    this.loadPlaylists();
                }, 1000);
            } else {
                this.showMessage(`❌ ${result.detail || 'Error adding playlist'}`, 'error');
            }
        } catch (error) {
            console.error('Network error:', error);
            this.showMessage('❌ Network error: ' + error.message, 'error');
        } finally {
            submitButton.disabled = false;
            submitButton.textContent = originalText;
        }
    }


    showMessage(message, type) {
        // Remove existing messages
        const existingMessages = document.querySelectorAll('.success-message, .error-message');
        existingMessages.forEach(msg => msg.remove());

        // Create new message
        const messageDiv = document.createElement('div');
        messageDiv.className = `${type}-message`;
        messageDiv.textContent = message;

        // Insert at the top of container
        const container = document.querySelector('.container');
        container.insertBefore(messageDiv, container.firstChild);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (messageDiv.parentNode) {
                messageDiv.remove();
            }
        }, 5000);

        // Scroll to top to show message
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    new PlaylistDownloader();
});
