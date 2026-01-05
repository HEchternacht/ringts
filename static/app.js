// Global state
let allPlayers = [];
let selectedPlayers = [];
let dateRange = { min: null, max: null };
let rankingsData = [];
let consoleEventSource = null;
let consolePaused = false;
let deltasByDate = new Map();
let deltaIds = new Set(); // Track unique deltas to prevent duplicates

// Initialize app
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

async function initializeApp() {
    await loadPlayers();
    await loadDateRange();
    await loadTopPlayers();
    await loadRecentUpdates();
    setupEventListeners();
    connectConsoleStream();
    await loadDeltas(); // Initial load
    setInterval(loadDeltas, 60000); // Poll every 1 minute
    checkScraperStatus();
    setInterval(checkScraperStatus, 60000); // Check every 1 minute
}

// Setup event listeners
function setupEventListeners() {
    // Dashboard tab
    document.getElementById('playerSearch').addEventListener('input', filterPlayers);
    document.getElementById('generateVisualization').addEventListener('click', generateVisualization);
    document.getElementById('clearAll').addEventListener('click', clearAll);
    document.getElementById('clearDates').addEventListener('click', clearDates);
    
    // Manual Update button
    document.getElementById('manualUpdate').addEventListener('click', triggerManualUpdate);
    
    // Rankings tab
    document.getElementById('loadRankings').addEventListener('click', loadRankingsTable);
    document.getElementById('rankingsSearch').addEventListener('input', filterRankings);
    document.getElementById('sortColumn').addEventListener('change', sortRankings);
    document.getElementById('sortOrder').addEventListener('click', toggleSortOrder);
    
    // Console tab
    document.getElementById('clearConsole').addEventListener('click', clearConsole);
    document.getElementById('pauseConsole').addEventListener('click', toggleConsolePause);
    
    // Tab navigation
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    
    // Data management tab
    document.getElementById('uploadDeltas').addEventListener('change', handleDeltasUpload);
    document.getElementById('uploadExps').addEventListener('change', handleExpsUpload);
}

// Load players from API
async function loadPlayers() {
    try {
        const response = await fetch('/api/players');
        allPlayers = await response.json();
        renderPlayerList(allPlayers);
    } catch (error) {
        showError('Failed to load players: ' + error.message);
    }
}

// Load available date range
async function loadDateRange() {
    try {
        const response = await fetch('/api/date-range');
        dateRange = await response.json();
        
        if (dateRange.min && dateRange.max) {
            const startInput = document.getElementById('startDate');
            const endInput = document.getElementById('endDate');
            
            // Set min and max attributes
            startInput.min = dateRange.min.slice(0, 16);
            startInput.max = dateRange.max.slice(0, 16);
            endInput.min = dateRange.min.slice(0, 16);
            endInput.max = dateRange.max.slice(0, 16);
        }
    } catch (error) {
        console.error('Failed to load date range:', error);
    }
}

// Render player list
function renderPlayerList(players) {
    const playerList = document.getElementById('playerList');
    playerList.innerHTML = '';
    
    players.forEach(player => {
        const div = document.createElement('div');
        div.className = 'player-item';
        div.textContent = player;
        div.addEventListener('click', () => selectPlayer(player));
        playerList.appendChild(div);
    });
}

// Filter players based on search
function filterPlayers(event) {
    const searchTerm = event.target.value.toLowerCase();
    const filteredPlayers = allPlayers.filter(player => 
        player.toLowerCase().includes(searchTerm)
    );
    renderPlayerList(filteredPlayers);
}

// Select a player
function selectPlayer(player) {
    if (!selectedPlayers.includes(player)) {
        selectedPlayers.push(player);
        renderSelectedPlayers();
    }
}

// Remove a player from selection
function removePlayer(player) {
    selectedPlayers = selectedPlayers.filter(p => p !== player);
    renderSelectedPlayers();
}

// Render selected players
function renderSelectedPlayers() {
    const container = document.getElementById('selectedPlayers');
    container.innerHTML = '';
    
    if (selectedPlayers.length === 0) {
        container.innerHTML = '<p style="color: #6c757d; font-style: italic;">No players selected</p>';
        return;
    }
    
    selectedPlayers.forEach(player => {
        const tag = document.createElement('div');
        tag.className = 'player-tag';
        tag.innerHTML = `
            <span>${player}</span>
            <span class="remove" onclick="removePlayer('${player}')">×</span>
        `;
        container.appendChild(tag);
    });
}

// Generate visualization (graph + stats + comparison)
async function generateVisualization() {
    if (selectedPlayers.length === 0) {
        showError('Please select at least one player');
        return;
    }
    
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    showLoading();
    
    try {
        const response = await fetch('/api/graph', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                names: selectedPlayers,
                datetime1: startDate || null,
                datetime2: endDate || null
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            // Render graph
            const graphData = JSON.parse(data.graph);
            graphData.layout.height = 500;
            Plotly.newPlot('graphDiv', graphData.data, graphData.layout, {responsive: true});
            
            // Render stats
            renderStatsTable(data.stats);
            
            // Render comparison
            renderComparison(data.comparison);
        } else {
            showError(data.error || 'Failed to generate visualization');
        }
    } catch (error) {
        showError('Failed to generate visualization: ' + error.message);
    } finally {
        hideLoading();
    }
}

// Render comparison data
function renderComparison(comparison) {
    const container = document.getElementById('comparisonInfo');
    
    if (!comparison || comparison.length === 0) {
        container.innerHTML = '<div class="comparison-placeholder"><p>No comparison data available</p></div>';
        return;
    }
    
    let html = '<div class="comparison-cards">';
    
    comparison.forEach(player => {
        html += `
            <div class="comparison-card">
                <h3>${player.name}</h3>
                <div class="comparison-stat">
                    <span class="comparison-label">Rank:</span>
                    <span class="comparison-value"><span class="rank-badge">#${player.rank} of ${player.total_players}</span></span>
                </div>
                <div class="comparison-stat">
                    <span class="comparison-label">Percentile:</span>
                    <span class="comparison-value"><span class="percentile-badge">Top ${100 - player.percentile}%</span></span>
                </div>
                <div class="comparison-stat">
                    <span class="comparison-label">Period EXP:</span>
                    <span class="comparison-value">${formatNumber(player.total_exp_period)}</span>
                </div>
                <div class="comparison-stat">
                    <span class="comparison-label">Total EXP:</span>
                    <span class="comparison-value">${formatNumber(player.current_total_exp)}</span>
                </div>
            </div>
        `;
    });
    
    html += '</div>';
    container.innerHTML = html;
}

// Render statistics table
function renderStatsTable(stats) {
    const container = document.getElementById('statsTable');
    
    if (stats.length === 0) {
        container.innerHTML = '<div class="stats-placeholder"><p>No statistics available</p></div>';
        return;
    }
    
    let html = '<div class="stats-table"><table>';
    html += '<thead><tr>';
    html += '<th>Player</th>';
    html += '<th>Total EXP</th>';
    html += '<th>Average EXP</th>';
    html += '<th>Updates</th>';
    html += '<th>Max EXP</th>';
    html += '<th>Min EXP</th>';
    html += '</tr></thead><tbody>';
    
    stats.forEach(stat => {
        html += '<tr>';
        html += `<td><strong>${stat.name}</strong></td>`;
        html += `<td>${formatNumber(stat['Total EXP'])}</td>`;
        html += `<td>${formatNumber(stat['Average EXP'])}</td>`;
        html += `<td>${stat.Updates}</td>`;
        html += `<td>${formatNumber(stat['Max EXP'])}</td>`;
        html += `<td>${formatNumber(stat['Min EXP'])}</td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// Load top players
async function loadTopPlayers() {
    try {
        const response = await fetch('/api/top-players?limit=10');
        const topPlayers = await response.json();
        renderTopPlayers(topPlayers);
    } catch (error) {
        console.error('Failed to load top players:', error);
    }
}

// Render top players
function renderTopPlayers(players) {
    const container = document.getElementById('topPlayersList');
    
    if (players.length === 0) {
        container.innerHTML = '<p style="color: #6c757d; font-style: italic;">No data available</p>';
        return;
    }
    
    let html = '';
    players.forEach((player, index) => {
        const medal = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '';
        html += `
            <div class="player-rank-item">
                <span class="rank">${medal || (index + 1)}</span>
                <span class="player-name">${player.name}</span>
                <span class="exp-value">${formatNumber(player.total_exp)}</span>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// Load recent updates
async function loadRecentUpdates() {
    try {
        const response = await fetch('/api/recent-updates?limit=10');
        const updates = await response.json();
        renderRecentUpdates(updates);
    } catch (error) {
        console.error('Failed to load recent updates:', error);
    }
}

// Render recent updates
function renderRecentUpdates(updates) {
    const container = document.getElementById('recentUpdatesList');
    
    if (updates.length === 0) {
        container.innerHTML = '<p style="color: #6c757d; font-style: italic;">No updates available</p>';
        return;
    }
    
    let html = '';
    updates.forEach(update => {
        const date = new Date(update['update time']);
        const timeString = date.toLocaleString();
        html += `
            <div class="update-item">
                <div><strong>${update.name}</strong></div>
                <div>+${formatNumber(update.deltaexp)} EXP</div>
                <div class="update-time">${timeString}</div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// Clear all selections
function clearAll() {
    selectedPlayers = [];
    renderSelectedPlayers();
    document.getElementById('playerSearch').value = '';
    renderPlayerList(allPlayers);
    clearDates();
    
    // Clear results
    document.getElementById('graphDiv').innerHTML = '<p style="padding: 20px; text-align: center; color: #6c757d; font-style: italic;">Select players and click "Generate Visualization" to view data</p>';
    document.getElementById('statsTable').innerHTML = '<div class="stats-placeholder"><p>Statistical breakdown will appear here</p></div>';
    document.getElementById('comparisonInfo').innerHTML = '<div class="comparison-placeholder"><p>Click "Generate Visualization" to see rankings and comparisons</p></div>';
}

// Clear dates
function clearDates() {
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
}

// Show loading overlay
function showLoading() {
    document.getElementById('loadingOverlay').classList.add('active');
}

// Hide loading overlay
function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('active');
}

// Show error message
function showError(message) {
    const errorDiv = document.getElementById('errorMessage');
    errorDiv.textContent = message;
    errorDiv.classList.add('active');
    
    setTimeout(() => {
        errorDiv.classList.remove('active');
    }, 5000);
}

// Format number with commas
function formatNumber(num) {
    return num.toLocaleString('en-US');
}

// Make removePlayer globally accessible
window.removePlayer = removePlayer;

// Tab switching
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`${tabName}-tab`).classList.add('active');
}

// Scraper status
async function checkScraperStatus() {
    try {
        const response = await fetch('/api/scraper-status');
        const data = await response.json();
        
        const statusDiv = document.getElementById('scraperStatus');
        const indicator = statusDiv.querySelector('.status-indicator');
        const text = statusDiv.querySelector('.status-text');
        const updateButton = document.getElementById('manualUpdate');
        
        // Update last check and last update times
        const lastUpdateTime = document.getElementById('lastUpdateTime');
        const lastCheckTime = document.getElementById('lastCheckTime');
        
        if (data.last_update) {
            lastUpdateTime.textContent = new Date(data.last_update).toLocaleString();
        }
        
        if (data.last_check) {
            lastCheckTime.textContent = new Date(data.last_check).toLocaleString();
        }
        
        if (data.running) {
            indicator.className = 'status-indicator running';
            
            // Update button based on state
            if (data.state === 'checking') {
                updateButton.disabled = true;
                updateButton.classList.add('verifying');
                updateButton.classList.remove('updating');
                updateButton.textContent = '🔍 Verifying Data...';
                text.textContent = 'Scraper: Verifying Data';
            } else if (data.state === 'scraping') {
                updateButton.disabled = true;
                updateButton.classList.add('updating');
                updateButton.classList.remove('verifying');
                updateButton.textContent = '⚙️ Running Scrappy...';
                text.textContent = 'Scraper: Collecting Data';
            } else if (data.state === 'sleeping') {
                updateButton.disabled = false;
                updateButton.classList.remove('updating', 'verifying');
                updateButton.textContent = '🔄 Update Now';
                text.textContent = 'Scraper: Idle (Waiting)';
            } else {
                // idle state
                updateButton.disabled = false;
                updateButton.classList.remove('updating', 'verifying');
                updateButton.textContent = '🔄 Update Now';
                text.textContent = 'Scraper Active';
            }
        } else {
            indicator.className = 'status-indicator error';
            text.textContent = 'Scraper Offline';
            updateButton.disabled = false;
            updateButton.classList.remove('updating', 'verifying');
            updateButton.textContent = '🔄 Update Now';
        }
    } catch (error) {
        console.error('Failed to check scraper status:', error);
    }
}

// Console stream
function connectConsoleStream() {
    consoleEventSource = new EventSource('/api/console-stream');
    
    consoleEventSource.onmessage = function(event) {
        if (!consolePaused) {
            addConsoleLog(event.data);
        }
    };
    
    consoleEventSource.onerror = function(error) {
        console.error('Console stream error:', error);
        addConsoleLog('[ERROR] Console stream disconnected. Reconnecting...', 'error');
        setTimeout(() => {
            consoleEventSource.close();
            connectConsoleStream();
        }, 3000);
    };
}

function addConsoleLog(message, type = 'info') {
    const consoleOutput = document.getElementById('consoleOutput');
    const line = document.createElement('div');
    line.className = `console-line ${type}`;
    line.textContent = message;
    consoleOutput.appendChild(line);
    
    // Auto-scroll if enabled
    if (document.getElementById('autoScroll').checked) {
        consoleOutput.scrollTop = consoleOutput.scrollHeight;
    }
    
    // Limit console lines to 1000
    while (consoleOutput.children.length > 1000) {
        consoleOutput.removeChild(consoleOutput.firstChild);
    }
}

function clearConsole() {
    document.getElementById('consoleOutput').innerHTML = '';
}

function toggleConsolePause() {
    consolePaused = !consolePaused;
    const btn = document.getElementById('pauseConsole');
    btn.textContent = consolePaused ? '▶️ Resume' : '⏸️ Pause';
}

// Rankings Table
async function loadRankingsTable() {
    const startDate = document.getElementById('rankStartDate').value;
    const endDate = document.getElementById('rankEndDate').value;
    
    // Allow empty dates for all-time data
    showLoading();
    
    try {
        const response = await fetch('/api/rankings-table', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                datetime1: startDate,
                datetime2: endDate
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            rankingsData = data.rankings;
            renderRankingsTable(rankingsData);
        } else {
            showError(data.error || 'Failed to load rankings');
        }
    } catch (error) {
        showError('Failed to load rankings: ' + error.message);
    } finally {
        hideLoading();
    }
}

function renderRankingsTable(data) {
    const container = document.getElementById('rankingsTableContent');
    
    if (data.length === 0) {
        container.innerHTML = '<div class="rankings-placeholder"><p>No data available for selected date range</p></div>';
        return;
    }
    
    let html = '<div class="rankings-data-table"><table>';
    html += '<thead><tr>';
    html += '<th>#</th>';
    html += '<th>Player</th>';
    html += '<th>Total EXP</th>';
    html += '<th>Updates</th>';
    html += '<th>Average EXP</th>';
    html += '<th>Max EXP</th>';
    html += '<th>Min EXP</th>';
    html += '</tr></thead><tbody>';
    
    data.forEach((player, index) => {
        const medal = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '';
        html += '<tr>';
        html += `<td class="rank-cell">${medal || (index + 1)}</td>`;
        html += `<td class="player-name-cell" onclick="showPlayerGraph('${player.name.replace(/'/g, "\\'")}')"><strong>${player.name}</strong></td>`;
        html += `<td>${formatNumber(player.total_exp)}</td>`;
        html += `<td>${player.updates}</td>`;
        html += `<td>${formatNumber(player.avg_exp)}</td>`;
        html += `<td>${formatNumber(player.max_exp)}</td>`;
        html += `<td>${formatNumber(player.min_exp)}</td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

function filterRankings(event) {
    const searchTerm = event.target.value.toLowerCase();
    const filteredData = rankingsData.filter(player => 
        player.name.toLowerCase().includes(searchTerm)
    );
    renderRankingsTable(filteredData);
}

function sortRankings() {
    const column = document.getElementById('sortColumn').value;
    const order = document.getElementById('sortOrder').dataset.order;
    
    const sortedData = [...rankingsData].sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];
        
        if (typeof aVal === 'string') {
            aVal = aVal.toLowerCase();
            bVal = bVal.toLowerCase();
        }
        
        if (order === 'asc') {
            return aVal > bVal ? 1 : -1;
        } else {
            return aVal < bVal ? 1 : -1;
        }
    });
    
    renderRankingsTable(sortedData);
}

function toggleSortOrder() {
    const btn = document.getElementById('sortOrder');
    const currentOrder = btn.dataset.order;
    const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
    
    btn.dataset.order = newOrder;
    btn.textContent = newOrder === 'asc' ? '⬆️ Ascending' : '⬇️ Descending';
    
    sortRankings();
}

// Player Graph Modal
async function showPlayerGraph(playerName) {
    const startDate = document.getElementById('rankStartDate').value;
    const endDate = document.getElementById('rankEndDate').value;
    
    showLoading();
    
    try {
        let url = `/api/player-graph/${encodeURIComponent(playerName)}`;
        if (startDate && endDate) {
            url += `?datetime1=${startDate}&datetime2=${endDate}`;
        }
        
        const response = await fetch(url);
        const data = await response.json();
        
        if (response.ok) {
            document.getElementById('modalPlayerName').textContent = `📊 ${playerName} - EXP History`;
            const graphData = JSON.parse(data.graph);
            Plotly.newPlot('modalGraphDiv', graphData.data, graphData.layout, {responsive: true});
            document.getElementById('playerModal').classList.add('active');
        } else {
            showError(data.error || 'Failed to load player graph');
        }
    } catch (error) {
        showError('Failed to load player graph: ' + error.message);
    } finally {
        hideLoading();
    }
}

function closePlayerModal() {
    document.getElementById('playerModal').classList.remove('active');
}

// Close modal on outside click
window.addEventListener('click', function(event) {
    const modal = document.getElementById('playerModal');
    if (event.target === modal) {
        closePlayerModal();
    }
});

// Make functions globally accessible
window.removePlayer = removePlayer;
window.showPlayerGraph = showPlayerGraph;
window.closePlayerModal = closePlayerModal;

// Delta Polling
async function loadDeltas() {
    try {
        const response = await fetch('/api/delta?limit=100');
        const data = await response.json();
        
        if (data.deltas && data.deltas.length > 0) {
            // Process deltas
            data.deltas.forEach(delta => {
                addDeltaToFeed(delta);
            });
        }
    } catch (error) {
        console.error('Error loading deltas:', error);
    }
}

function addDeltaToFeed(delta) {
    // Create unique ID for this delta
    const deltaId = `${delta.name}_${delta.update_time}_${delta.deltaexp}`;
    
    // Check if we already have this delta
    if (deltaIds.has(deltaId)) {
        return; // Skip duplicate
    }
    
    // Mark this delta as seen
    deltaIds.add(deltaId);
    
    const feedContent = document.getElementById('liveFeedContent');
    const deltaDate = new Date(delta.update_time);
    const prevDate = new Date(delta.prev_update_time);
    
    // Create time range key for grouping
    const prevTimeStr = prevDate.toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric', 
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
    
    const currentTimeStr = deltaDate.toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric', 
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
    
    // Use time range as the key (or just current time if same as prev)
    const dateTimeKey = prevTimeStr === currentTimeStr 
        ? currentTimeStr 
        : `${prevTimeStr} → ${currentTimeStr}`;
    
    // Initialize datetime group if needed
    if (!deltasByDate.has(dateTimeKey)) {
        deltasByDate.set(dateTimeKey, []);
    }
    
    // Add delta to array
    deltasByDate.get(dateTimeKey).push({
        name: delta.name,
        deltaexp: delta.deltaexp,
        update_time: delta.update_time,
        prev_update_time: delta.prev_update_time,
        timestamp: deltaDate.getTime(),
        id: deltaId
    });
    
    // Sort deltas by exp descending within the datetime group
    deltasByDate.get(dateTimeKey).sort((a, b) => b.deltaexp - a.deltaexp);
    
    // Rebuild the feed
    renderDeltaFeed();
    
    // Highlight new items briefly
    setTimeout(() => {
        const items = feedContent.querySelectorAll('.delta-item.new');
        items.forEach(item => item.classList.remove('new'));
    }, 3000);
}

function renderDeltaFeed() {
    const feedContent = document.getElementById('liveFeedContent');
    
    // Sort datetime keys descending (most recent first)
    // Need to sort by the actual timestamp, not the string
    const sortedDateTimes = Array.from(deltasByDate.keys()).sort((a, b) => {
        // Get the first delta from each group to compare timestamps
        const aDeltas = deltasByDate.get(a);
        const bDeltas = deltasByDate.get(b);
        const aTime = aDeltas.length > 0 ? aDeltas[0].timestamp : 0;
        const bTime = bDeltas.length > 0 ? bDeltas[0].timestamp : 0;
        return bTime - aTime; // Descending
    });
    
    let html = '';
    
    for (const dateTimeKey of sortedDateTimes) {
        const deltas = deltasByDate.get(dateTimeKey);
        
        html += `<div class="delta-date-group">`;
        html += `<div class="delta-date-header">${dateTimeKey}</div>`;
        
        for (const delta of deltas) {
            const currentTime = new Date(delta.update_time).toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
            
            const prevTime = new Date(delta.prev_update_time).toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
            
            // Show time range
            const timeRange = prevTime === currentTime ? currentTime : `${prevTime} → ${currentTime}`;
            
            html += `
                <div class="delta-item new" onclick="showPlayerGraph('${delta.name.replace(/'/g, "\\'")}')">
                    <div class="delta-player-name">${delta.name}</div>
                    <div class="delta-exp-value">
                        <span class="plus">+</span>${formatNumber(delta.deltaexp)} EXP
                    </div>
                    <div class="delta-time">${timeRange}</div>
                </div>
            `;
        }
        
        html += `</div>`;
    }
    
    feedContent.innerHTML = html || '<div class="live-feed-placeholder"><p>Waiting for updates...</p></div>';
}

// Manual Update Function
async function triggerManualUpdate() {
    const button = document.getElementById('manualUpdate');
    
    // Check current scraper state first
    try {
        const statusResponse = await fetch('/api/scraper-status');
        const statusData = await statusResponse.json();
        
        if (statusData.state === 'checking' || statusData.state === 'scraping') {
            showNotification('⚠️ Scraper is already running. Please wait...', 'warning');
            return;
        }
    } catch (error) {
        console.error('Error checking scraper status:', error);
    }
    
    button.disabled = true;
    button.classList.add('verifying');
    button.textContent = '🔍 Verifying Data...';

    try {
        const response = await fetch('/api/manual-update', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const data = await response.json();

        if (data.success) {
            showNotification('✅ Rankings updated successfully!', 'success');
            // Optionally reload players and rankings
            await loadPlayers();
            await loadRecentUpdates();
            await loadDeltas();
        } else if (response.status === 409) {
            showNotification(`⚠️ ${data.message}`, 'warning');
        } else {
            showNotification(`❌ Update failed: ${data.message}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Update failed: ${error.message}`, 'error');
    } finally {
        // Button state will be updated by next status check
        setTimeout(() => {
            checkScraperStatus();
        }, 1000);
    }
}

// Notification function
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 20px;
        background: ${type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : '#007bff'};
        color: white;
        border-radius: 5px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.3);
        z-index: 10000;
        animation: slideIn 0.3s ease-out;
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease-in';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// Data Management Functions
async function handleDeltasUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    const fileNameSpan = document.getElementById('deltasFileName');
    const statusDiv = document.getElementById('deltasStatus');
    const passwordInput = document.getElementById('deltasPassword');
    const password = passwordInput.value.trim();
    
    if (!password) {
        statusDiv.textContent = '❌ Error: Password is required';
        statusDiv.className = 'status-message error';
        showNotification('❌ Please enter password', 'error');
        event.target.value = '';
        return;
    }
    
    fileNameSpan.textContent = file.name;
    statusDiv.textContent = 'Uploading...';
    statusDiv.className = 'status-message info';
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('password', password);
    
    try {
        const response = await fetch('/api/upload/deltas', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            statusDiv.textContent = `✅ Successfully uploaded ${data.records} records. Backup created.`;
            statusDiv.className = 'status-message success';
            showNotification('✅ Deltas file uploaded successfully!', 'success');
            passwordInput.value = ''; // Clear password
        } else if (response.status === 401) {
            statusDiv.textContent = '❌ Error: Invalid password';
            statusDiv.className = 'status-message error';
            showNotification('❌ Invalid password', 'error');
        } else {
            statusDiv.textContent = `❌ Error: ${data.error}`;
            statusDiv.className = 'status-message error';
            showNotification(`❌ Upload failed: ${data.error}`, 'error');
        }
    } catch (error) {
        statusDiv.textContent = `❌ Error: ${error.message}`;
        statusDiv.className = 'status-message error';
        showNotification(`❌ Upload failed: ${error.message}`, 'error');
    }
    
    // Clear the file input
    event.target.value = '';
}

async function handleExpsUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    const fileNameSpan = document.getElementById('expsFileName');
    const statusDiv = document.getElementById('expsStatus');
    const passwordInput = document.getElementById('expsPassword');
    const password = passwordInput.value.trim();
    
    if (!password) {
        statusDiv.textContent = '❌ Error: Password is required';
        statusDiv.className = 'status-message error';
        showNotification('❌ Please enter password', 'error');
        event.target.value = '';
        return;
    }
    
    fileNameSpan.textContent = file.name;
    statusDiv.textContent = 'Uploading...';
    statusDiv.className = 'status-message info';
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('password', password);
    
    try {
        const response = await fetch('/api/upload/exps', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            statusDiv.textContent = `✅ Successfully uploaded ${data.records} records. Backup created.`;
            statusDiv.className = 'status-message success';
            showNotification('✅ Exps file uploaded successfully!', 'success');
            passwordInput.value = ''; // Clear password
        } else if (response.status === 401) {
            statusDiv.textContent = '❌ Error: Invalid password';
            statusDiv.className = 'status-message error';
            showNotification('❌ Invalid password', 'error');
        } else {
            statusDiv.textContent = `❌ Error: ${data.error}`;
            statusDiv.className = 'status-message error';
            showNotification(`❌ Upload failed: ${data.error}`, 'error');
        }
    } catch (error) {
        statusDiv.textContent = `❌ Error: ${error.message}`;
        statusDiv.className = 'status-message error';
        showNotification(`❌ Upload failed: ${error.message}`, 'error');
    }
    
    // Clear the file input
    event.target.value = '';
}
