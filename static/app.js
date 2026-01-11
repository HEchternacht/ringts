// Global state
let allPlayers = [];
let selectedPlayers = [];
let dateRange = { min: null, max: null };
let rankingsData = [];
let consoleEventSource = null;
let consolePaused = false;
let deltasByDate = new Map();
let deltaIds = new Set(); // Track unique deltas to prevent duplicates
let currentFilters = { world: 'Auroria', guild: 'Ascended Auroria' }; // Global filters

// Carousel state
let currentComparisonIndex = 0;
let currentPlayerDetailsIndex = 0;
let allComparisons = [];
let selectedPlayersForCarousel = [];

// Player details cache
let playerDetailsCache = {};

// Initialize app
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

async function initializeApp() {
    await loadFiltersConfig(); // Load available worlds and guilds
    await loadPlayers();
    await loadDateRange();
    await loadTopPlayers();
    await loadRecentUpdates();
    await loadMakersDashboard(); // Load makers cards
    setupEventListeners();
    connectConsoleStream();
    await loadDeltas(); // Initial load
    setInterval(loadDeltas, 60000); // Poll every 1 minute
    checkScraperStatus();
    setInterval(checkScraperStatus, 60000); // Check every 1 minute
}

// Setup event listeners
function setupEventListeners() {
    // Global filters
    document.getElementById('applyFilters').addEventListener('click', applyFilters);
    
    // Dashboard tab
    document.getElementById('playerSearch').addEventListener('input', filterPlayers);
    document.getElementById('generateVisualization').addEventListener('click', generateVisualization);
    document.getElementById('clearAll').addEventListener('click', clearAll);
    document.getElementById('clearDates').addEventListener('click', clearDates);
    
    // Carousel navigation
    document.getElementById('comparisonPrevBtn').addEventListener('click', () => navigateCarousel('comparison', -1));
    document.getElementById('comparisonNextBtn').addEventListener('click', () => navigateCarousel('comparison', 1));
    document.getElementById('playerDetailsPrevBtn').addEventListener('click', () => navigateCarousel('playerDetails', -1));
    document.getElementById('playerDetailsNextBtn').addEventListener('click', () => navigateCarousel('playerDetails', 1));
    
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
    
    // Health status click handler
    document.getElementById('healthStatus').addEventListener('click', showHealthDetails);
    
    // Close health modal on outside click
    document.getElementById('healthModal').addEventListener('click', function(e) {
        if (e.target === this) {
            closeHealthModal();
        }
    });
}

// Load filters configuration (worlds and guilds)
async function loadFiltersConfig() {
    try {
        const response = await fetch('/api/scraping-config');
        const config = await response.json();
        
        const worldFilter = document.getElementById('worldFilter');
        const guildFilter = document.getElementById('guildFilter');
        
        // Get unique worlds and guilds
        const worlds = new Set();
        const guildsByWorld = {};
        
        config.forEach(item => {
            worlds.add(item.world);
            if (!guildsByWorld[item.world]) {
                guildsByWorld[item.world] = [];
            }
            guildsByWorld[item.world].push(...item.guilds);
        });
        
        // Populate world dropdown
        worldFilter.innerHTML = '';
        Array.from(worlds).sort().forEach(world => {
            const option = document.createElement('option');
            option.value = world;
            option.textContent = world;
            if (world === currentFilters.world) {
                option.selected = true;
            }
            worldFilter.appendChild(option);
        });
        
        // Update guild dropdown based on selected world
        updateGuildFilter(guildsByWorld);
        
        // Listen for world changes to update guild options
        worldFilter.addEventListener('change', () => {
            updateGuildFilter(guildsByWorld);
        });
        
        window.guildsByWorld = guildsByWorld; // Store for later use
    } catch (error) {
        console.error('Failed to load filters config:', error);
    }
}

function updateGuildFilter(guildsByWorld) {
    const worldFilter = document.getElementById('worldFilter');
    const guildFilter = document.getElementById('guildFilter');
    const selectedWorld = worldFilter.value;
    
    const guilds = guildsByWorld[selectedWorld] || [];
    
    guildFilter.innerHTML = '';
    guilds.forEach(guild => {
        const option = document.createElement('option');
        option.value = guild;
        option.textContent = guild;
        if (guild === currentFilters.guild && guilds.includes(currentFilters.guild)) {
            option.selected = true;
        }
        guildFilter.appendChild(option);
    });
}

// Apply filters
async function applyFilters() {
    const worldFilter = document.getElementById('worldFilter');
    const guildFilter = document.getElementById('guildFilter');
    
    currentFilters.world = worldFilter.value;
    currentFilters.guild = guildFilter.value;
    
    // Reload all data with new filters
    await loadPlayers();
    await loadTopPlayers();
    await loadRecentUpdates();
    await loadDeltas();
    
    // Clear current visualization
    selectedPlayers = [];
    renderSelectedPlayers();
    document.getElementById('graphDiv').innerHTML = '<p>Select players and click "Generate Visualization" to view data</p>';
    document.getElementById('statsTable').innerHTML = '';
    document.getElementById('comparisonTable').innerHTML = '';
    
    showSuccess(`Filters applied: ${currentFilters.world} - ${currentFilters.guild}`);
}

// Load players from API
async function loadPlayers() {
    try {
        const params = new URLSearchParams({
            world: currentFilters.world,
            guild: currentFilters.guild
        });
        const response = await fetch(`/api/players?${params}`);
        allPlayers = await response.json();
        renderPlayerList(allPlayers);
    } catch (error) {
        showError('Failed to load players: ' + error.message);
    }
}

// Load available date range
async function loadDateRange() {
    try {
        const params = new URLSearchParams({
            world: currentFilters.world,
            guild: currentFilters.guild
        });
        const response = await fetch(`/api/date-range?${params}`);
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
        // Reset carousel state
        currentComparisonIndex = 0;
        currentPlayerDetailsIndex = 0;
        allComparisons = [];
        selectedPlayersForCarousel = [...selectedPlayers];
        
        // Clear cache to avoid bugs with stale data
        playerDetailsCache = {};
        
        // Fetch combined data for all selected players
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
            // Render combined graph for all players
            const graphData = JSON.parse(data.graph);
            graphData.layout.height = 600;
            graphData.layout.autosize = true;
            Plotly.newPlot('graphDiv', graphData.data, graphData.layout, {
                responsive: true,
                displayModeBar: true,
                displaylogo: false
            });
            
            // Store comparison data for carousel
            allComparisons = data.comparison || [];
            
            // Render carousel for comparisons
            renderComparisonCarousel();
            
            // Render stats
            renderStatsTable(data.stats);
            
            // Load player details for carousel
            await loadDashboardPlayerDetails(selectedPlayers);
        } else {
            showError(data.error || 'Failed to generate visualization');
        }
    } catch (error) {
        showError('Failed to generate visualization: ' + error.message);
    } finally {
        hideLoading();
    }
}

// Render comparison carousel
function renderComparisonCarousel() {
    const carousel = document.getElementById('comparisonCarousel');
    const title = document.getElementById('comparisonTitle');
    const prevBtn = document.getElementById('comparisonPrevBtn');
    const nextBtn = document.getElementById('comparisonNextBtn');
    
    // Clear existing content
    carousel.innerHTML = '';
    
    if (allComparisons.length === 0) {
        carousel.innerHTML = '<div class="carousel-item active"><p>No comparison data available</p></div>';
        prevBtn.style.visibility = 'hidden';
        nextBtn.style.visibility = 'hidden';
        return;
    }
    
    // Create carousel items for each player comparison
    allComparisons.forEach((comparison, index) => {
        const item = document.createElement('div');
        item.className = `carousel-item ${index === 0 ? 'active' : ''}`;
        item.innerHTML = renderSingleComparison(comparison);
        carousel.appendChild(item);
    });
    
    // Update title and navigation buttons
    title.textContent = `🏆 ${allComparisons[0].name} - Comparison`;
    prevBtn.style.visibility = allComparisons.length > 1 ? 'visible' : 'hidden';
    nextBtn.style.visibility = allComparisons.length > 1 ? 'visible' : 'hidden';
}

// Render single comparison card
function renderSingleComparison(player) {
    return `
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
}

// Navigate carousel with looping
function navigateCarousel(type, direction) {
    if (type === 'comparison') {
        if (allComparisons.length <= 1) return;
        
        const newIndex = (currentComparisonIndex + direction + allComparisons.length) % allComparisons.length;
        const carousel = document.getElementById('comparisonCarousel');
        const items = carousel.querySelectorAll('.carousel-item');
        const title = document.getElementById('comparisonTitle');
        
        // Hide current item
        items[currentComparisonIndex].classList.remove('active');
        
        // Show new item
        currentComparisonIndex = newIndex;
        items[currentComparisonIndex].classList.add('active');
        
        // Update title
        title.textContent = `🏆 ${allComparisons[currentComparisonIndex].name} - Comparison`;
        
    } else if (type === 'playerDetails') {
        if (selectedPlayersForCarousel.length <= 1) return;
        
        const newIndex = (currentPlayerDetailsIndex + direction + selectedPlayersForCarousel.length) % selectedPlayersForCarousel.length;
        
        // Update current index
        currentPlayerDetailsIndex = newIndex;
        
        // Load the player details for the new index
        const playerName = selectedPlayersForCarousel[currentPlayerDetailsIndex];
        loadSinglePlayerDetails(playerName);
    }
}

async function loadSinglePlayerDetails(playerName) {
    const dashboardPlayerName = document.getElementById('dashboardPlayerName');
    const loadingEl = document.getElementById('dashboardDetailsLoading');
    const contentEl = document.getElementById('dashboardDetailsContent');
    const multiGraphDiv = document.getElementById('dashboardMultiGraphDiv');
    
    // Update player name first
    dashboardPlayerName.textContent = `📋 ${playerName}`;
    
    // Switch to multi-graph tab automatically
    switchDashboardMainTab('multi-graph');
    
    // Always clear the multi-graph AND details content first to prevent old data from showing
    if (multiGraphDiv) {
        multiGraphDiv.innerHTML = '';
        // Also purge any Plotly data
        if (multiGraphDiv.data) {
            Plotly.purge(multiGraphDiv);
        }
    }
    // Clear details content as well
    contentEl.innerHTML = '';
    contentEl.style.display = 'none';
    
    // Check if data is cached
    if (playerDetailsCache[playerName]) {
        // Show loading briefly
        loadingEl.style.display = 'flex';
        contentEl.style.display = 'none';
        
        // Use cached data with small delay
        setTimeout(() => {
            contentEl.innerHTML = playerDetailsCache[playerName].html;
            contentEl.style.display = 'block';
            loadingEl.style.display = 'none';
            
            // Re-render the multi-graph from cache
            if (playerDetailsCache[playerName].multiGraphData) {
                setTimeout(() => {
                    renderCombinedOverview(playerDetailsCache[playerName].multiGraphData, 'dashboardMultiGraphDiv', playerName);
                }, 50);
            }
        }, 100);
        return;
    }
    
    // Show loading and clear old content with a small delay to ensure UI updates
    setTimeout(async () => {
        loadingEl.style.display = 'flex';
        contentEl.style.display = 'none';
        contentEl.innerHTML = '';
        
        // Small delay to ensure loading animation is visible
        await new Promise(resolve => setTimeout(resolve, 50));
        
        // Load details with dashboard context and cache the result
        await loadPlayerDetailsWithCache(playerName, 'dashboard');
        
        // Force refresh the multi-graph view after loading
        setTimeout(() => {
            const multiGraphDiv = document.getElementById('dashboardMultiGraphDiv');
            if (multiGraphDiv && multiGraphDiv.data) {
                Plotly.Plots.resize(multiGraphDiv);
            }
        }, 100);
    }, 0);
}

async function loadDashboardPlayerDetails(playerNames) {
    const dashboardSection = document.getElementById('playerDetailsDashboard');
    const dashboardPlayerName = document.getElementById('dashboardPlayerName');
    const prevBtn = document.getElementById('playerDetailsPrevBtn');
    const nextBtn = document.getElementById('playerDetailsNextBtn');
    
    if (playerNames.length === 0) {
        dashboardSection.style.display = 'none';
        return;
    }
    
    dashboardSection.style.display = 'block';
    
    // Show/hide navigation buttons
    prevBtn.style.visibility = playerNames.length > 1 ? 'visible' : 'hidden';
    nextBtn.style.visibility = playerNames.length > 1 ? 'visible' : 'hidden';
    
    // Load the first player
    currentPlayerDetailsIndex = 0;
    const playerName = playerNames[0];
    dashboardPlayerName.textContent = `📋 ${playerName}`;
    
    // Load details with dashboard context
    await loadPlayerDetailsWithCache(playerName, 'dashboard');
}

async function loadPlayerDetailsWithCache(playerName, context = 'modal') {
    const loadingId = context === 'modal' ? 'playerDetailsLoading' : 'dashboardDetailsLoading';
    const contentId = context === 'modal' ? 'playerDetailsContent' : 'dashboardDetailsContent';
    
    const loadingEl = document.getElementById(loadingId);
    const contentEl = document.getElementById(contentId);
    
    loadingEl.style.display = 'flex';
    contentEl.style.display = 'none';
    
    try {
        const response = await fetch(`/api/player-details/${encodeURIComponent(playerName)}`);
        const data = await response.json();
        
        if (data.success && data.tables.length > 0) {
            const html = renderPlayerDetails(data, context);
            contentEl.innerHTML = html;
            contentEl.style.display = 'block';
            
            // Cache the data
            playerDetailsCache[playerName] = {
                html: html,
                multiGraphData: data
            };
        } else {
            contentEl.innerHTML = '<p class="no-data">No detailed data available for this player.</p>';
            contentEl.style.display = 'block';
        }
    } catch (error) {
        contentEl.innerHTML = `<p class="error">Failed to load player details: ${error.message}</p>`;
        contentEl.style.display = 'block';
    } finally {
        loadingEl.style.display = 'none';
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

// Load makers dashboard cards
async function loadMakersDashboard() {
    const container = document.getElementById('makersDashboardCards');
    
    if (!container) {
        console.warn('Makers dashboard container not found');
        return;
    }
    
    // Show loading
    container.innerHTML = `
        <div class="makers-loading">
            <div class="loading-spinner"></div>
            <p>Loading makers...</p>
        </div>
    `;
    
    try {
        const response = await fetch('/api/makers/list');
        const data = await response.json();
        
        if (data.makers && data.makers.length > 0) {
            container.innerHTML = '';
            
            for (const maker of data.makers) {
                const card = await createMakerCard(maker);
                container.appendChild(card);
            }
        } else {
            container.innerHTML = '<p style="grid-column: 1 / -1; text-align: center; color: #6c757d; font-style: italic; padding: 40px;">No makers configured</p>';
        }
    } catch (error) {
        console.error('Failed to load makers dashboard:', error);
        container.innerHTML = '<p style="grid-column: 1 / -1; text-align: center; color: #dc3545; padding: 40px;">Failed to load makers</p>';
    }
}

// Create a maker card element
async function createMakerCard(maker) {
    const card = document.createElement('div');
    card.className = 'makers-card';
    card.onclick = () => window.location.href = '/makers';
    
    // Get maker data for stats
    let makerData = null;
    try {
        const response = await fetch(`/api/makers/graph`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: maker.name, world: maker.world })
        });
        
        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                makerData = data.stats;
            }
        }
    } catch (error) {
        console.error('Failed to load maker stats:', error);
    }
    
    // Get latest online status from deltas
    let latestDelta = null;
    try {
        const response = await fetch(`/api/makers/deltas?name=${encodeURIComponent(maker.name)}&world=${maker.world}&limit=1`);
        if (response.ok) {
            const data = await response.json();
            if (data.deltas && data.deltas.length > 0) {
                latestDelta = data.deltas[0];
            }
        }
    } catch (error) {
        console.error('Failed to load maker deltas:', error);
    }
    
    const isOnline = latestDelta && latestDelta.delta_online > 0;
    const onlineTime = latestDelta ? formatOnlineTime(latestDelta.delta_online) : '0h 0m';
    
    card.innerHTML = `
        <div class="makers-card-header">
            <div class="makers-card-name">🔧 ${maker.name}</div>
            <div class="makers-card-world">${maker.world}</div>
        </div>
        <div class="makers-card-status">
            <div class="makers-status-indicator ${isOnline ? 'online' : 'offline'}"></div>
            <div class="makers-status-text">${isOnline ? 'Online' : 'Offline'}</div>
        </div>
        <div class="makers-card-stats">
            <div class="makers-stat">
                <div class="makers-stat-label">Total EXP</div>
                <div class="makers-stat-value">${makerData ? formatNumber(makerData.total_exp) : '0'}</div>
            </div>
            <div class="makers-stat">
                <div class="makers-stat-label">Online Time</div>
                <div class="makers-stat-value online-time">${onlineTime}</div>
            </div>
        </div>
    `;
    
    return card;
}

// Format online time from minutes
function formatOnlineTime(minutes) {
    if (!minutes || minutes === 0) return '0h 0m';
    
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
}

// Clear all selections
function clearAll() {
    selectedPlayers = [];
    renderSelectedPlayers();
    document.getElementById('playerSearch').value = '';
    renderPlayerList(allPlayers);
    clearDates();
    
    // Reset carousel state
    currentComparisonIndex = 0;
    currentPlayerDetailsIndex = 0;
    allComparisons = [];
    selectedPlayersForCarousel = [];
    
    // Clear cache
    playerDetailsCache = {};
    
    // Clear results
    document.getElementById('graphDiv').innerHTML = '<p style="padding: 20px; text-align: center; color: #6c757d; font-style: italic;">Select players and click "Generate Visualization" to view data</p>';
    
    // Clear comparison carousel
    const comparisonCarousel = document.getElementById('comparisonCarousel');
    comparisonCarousel.innerHTML = '<div class="carousel-item active"><p>Click "Generate Visualization" to see rankings and comparisons</p></div>';
    document.getElementById('comparisonTitle').textContent = '🏆 Player Comparison';
    document.getElementById('comparisonPrevBtn').style.visibility = 'hidden';
    document.getElementById('comparisonNextBtn').style.visibility = 'hidden';
    
    document.getElementById('statsTable').innerHTML = '<div class="stats-placeholder"><p>Statistical breakdown will appear here</p></div>';
    
    // Hide player details section
    document.getElementById('playerDetailsDashboard').style.display = 'none';
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
        
        // Load world status data
        await loadWorldStatus();
        
        // Update health and memory status
        await updateHealthStatus();
        await updateMemoryStatus();
        
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

// Update health status
async function updateHealthStatus() {
    try {
        const response = await fetch('/healthz');
        const data = await response.json();
        
        const healthItem = document.getElementById('healthStatus');
        const healthText = healthItem.querySelector('.health-text');
        const healthIcon = healthItem.querySelector('.health-icon');
        
        if (data.status === 'healthy') {
            healthText.textContent = 'Health: ✓ Healthy';
            healthIcon.textContent = '💚';
            healthItem.className = 'health-item healthy';
        } else if (data.status === 'degraded') {
            healthText.textContent = 'Health: ⚠ Degraded';
            healthIcon.textContent = '💛';
            healthItem.className = 'health-item degraded';
        } else {
            healthText.textContent = 'Health: ✗ Unhealthy';
            healthIcon.textContent = '❤️';
            healthItem.className = 'health-item unhealthy';
        }
        
        // Store health data for modal
        healthItem.dataset.healthData = JSON.stringify(data);
    } catch (error) {
        console.error('Failed to check health:', error);
        const healthItem = document.getElementById('healthStatus');
        const healthText = healthItem.querySelector('.health-text');
        healthText.textContent = 'Health: Error';
        healthItem.className = 'health-item error';
    }
}

// Update memory status
async function updateMemoryStatus() {
    try {
        const response = await fetch('/memusage');
        const data = await response.json();
        
        const memoryItem = document.getElementById('memoryStatus');
        const memoryText = memoryItem.querySelector('.health-text');
        
        const processMB = data.process.rss_mb;
        const processPercent = data.process.percent;
        
        memoryText.textContent = `RAM: ${processMB}MB (${processPercent}%)`;
        
        // Color code based on percentage
        if (processPercent < 50) {
            memoryItem.className = 'health-item healthy';
        } else if (processPercent < 75) {
            memoryItem.className = 'health-item degraded';
        } else {
            memoryItem.className = 'health-item unhealthy';
        }
        
        // Store memory data for tooltip
        memoryItem.title = `Process: ${processMB}MB | System: ${data.system.used_mb}/${data.system.total_mb}MB (${data.system.percent}%)`;
    } catch (error) {
        console.error('Failed to check memory:', error);
        const memoryItem = document.getElementById('memoryStatus');
        const memoryText = memoryItem.querySelector('.health-text');
        memoryText.textContent = 'RAM: Error';
    }
}

// Show health details modal
function showHealthDetails() {
    const healthItem = document.getElementById('healthStatus');
    const healthData = JSON.parse(healthItem.dataset.healthData || '{}');
    
    const modal = document.getElementById('healthModal');
    const modalBody = document.getElementById('healthModalBody');
    
    let html = `<div class="health-details">`;
    html += `<div class="health-status-badge ${healthData.status}">${healthData.status.toUpperCase()}</div>`;
    
    if (healthData.reason) {
        html += `<div class="health-reason"><strong>Reason:</strong> ${healthData.reason}</div>`;
    }
    
    if (healthData.checks) {
        html += `<h4>Health Checks:</h4><div class="health-checks">`;
        
        for (const [key, value] of Object.entries(healthData.checks)) {
            const displayKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            let displayValue = value;
            
            if (typeof value === 'boolean') {
                displayValue = value ? '✓ Yes' : '✗ No';
            } else if (typeof value === 'number') {
                displayValue = value.toLocaleString();
            }
            
            html += `
                <div class="health-check-item">
                    <span class="check-label">${displayKey}:</span>
                    <span class="check-value ${typeof value === 'boolean' ? (value ? 'success' : 'error') : ''}">${displayValue}</span>
                </div>
            `;
        }
        
        html += `</div>`;
    }
    
    html += `</div>`;
    modalBody.innerHTML = html;
    modal.style.display = 'flex';
}

// Close health modal
function closeHealthModal() {
    const modal = document.getElementById('healthModal');
    modal.style.display = 'none';
}

// Load world status data
async function loadWorldStatus() {
    try {
        const response = await fetch('/api/status-data');
        if (!response.ok) {
            // Data not available yet
            return;
        }
        
        const data = await response.json();
        const worldStatusContent = document.getElementById('worldStatusContent');
        const fetchTimeElement = document.getElementById('fetchTime');
        
        // Update fetch time
        if (data.fetch_time) {
            const fetchDate = new Date(data.fetch_time);
            fetchTimeElement.textContent = fetchDate.toLocaleString();
        }
        
        // Render world status
        if (data.worlds && Object.keys(data.worlds).length > 0) {
            let html = '';
            
            // Sort worlds alphabetically
            const sortedWorlds = Object.keys(data.worlds).sort();
            
            for (const worldName of sortedWorlds) {
                const worldData = data.worlds[worldName];
                
                html += `<div class="world-status-item">`;
                html += `<div class="world-name">🌍 ${worldName}</div>`;
                html += `<div class="world-routines">`;
                
                for (const routine of worldData) {
                    const statusClass = routine.status.toLowerCase() === 'atualizado' ? 'atualizado' : 
                                       routine.status.toLowerCase() === 'ok' ? 'ok' : 'outdated';
                    
                    const updateTime = routine['last update'] ? 
                        new Date(routine['last update']).toLocaleTimeString() : '--';
                    
                    html += `
                        <div class="routine-row">
                            <div class="routine-name">${routine.rotina}</div>
                            <div class="routine-time">${updateTime}</div>
                            <div class="routine-status ${statusClass}">${routine.status}</div>
                        </div>
                    `;
                }
                
                html += `</div></div>`;
            }
            
            worldStatusContent.innerHTML = html;
        } else {
            worldStatusContent.innerHTML = '<p style="text-align: center; color: #6c757d;">No world data available</p>';
        }
    } catch (error) {
        console.error('Failed to load world status:', error);
        const worldStatusContent = document.getElementById('worldStatusContent');
        worldStatusContent.innerHTML = '<p style="text-align: center; color: #dc3545;">Error loading world status</p>';
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
    
    // Clear previous player's data immediately
    const modalGraphDiv = document.getElementById('modalGraphDiv');
    const modalMultiGraphDiv = document.getElementById('modalMultiGraphDiv');
    const playerDetailsContent = document.getElementById('playerDetailsContent');
    
    if (modalGraphDiv && modalGraphDiv.data) {
        Plotly.purge(modalGraphDiv);
    }
    if (modalMultiGraphDiv) {
        modalMultiGraphDiv.innerHTML = '';
        if (modalMultiGraphDiv.data) {
            Plotly.purge(modalMultiGraphDiv);
        }
    }
    if (playerDetailsContent) {
        playerDetailsContent.innerHTML = '';
        playerDetailsContent.style.display = 'none';
    }
    
    showLoading();
    
    try {
        let url = `/api/player-graph/${encodeURIComponent(playerName)}`;
        if (startDate && endDate) {
            url += `?datetime1=${startDate}&datetime2=${endDate}`;
        }
        
        const response = await fetch(url);
        const data = await response.json();
        
        if (response.ok) {
            // Store player name in modal data attribute
            const modal = document.getElementById('playerModal');
            modal.dataset.playerName = playerName;
            
            document.getElementById('modalPlayerName').textContent = `📊 ${playerName} - EXP History`;
            const graphData = JSON.parse(data.graph);
            // Show modal first so the container has correct size, then plot and trigger a resize
            modal.classList.add('active');
            Plotly.newPlot('modalGraphDiv', graphData.data, graphData.layout, {responsive: true});
            try {
                Plotly.Plots.resize(document.getElementById('modalGraphDiv'));
            } catch (e) {
                console.warn('Plotly resize failed:', e);
            }

            // Load player details
            await loadPlayerDetails(playerName, 'modal');
        } else {
            showError(data.error || 'Failed to load player graph');
        }
    } catch (error) {
        showError('Failed to load player graph: ' + error.message);
    } finally {
        hideLoading();
    }
}

async function loadPlayerDetails(playerName, context = 'modal') {
    const loadingId = context === 'modal' ? 'playerDetailsLoading' : 'dashboardDetailsLoading';
    const contentId = context === 'modal' ? 'playerDetailsContent' : 'dashboardDetailsContent';
    
    const loadingEl = document.getElementById(loadingId);
    const contentEl = document.getElementById(contentId);
    
    if (!loadingEl || !contentEl) {
        console.error('Loading or content element not found', { loadingId, contentId });
        return;
    }
    
    // Validate playerName
    if (!playerName || typeof playerName !== 'string') {
        console.error('Invalid player name:', playerName);
        contentEl.innerHTML = '<p class="error">Invalid player name</p>';
        contentEl.style.display = 'block';
        loadingEl.style.display = 'none';
        return;
    }
    
    console.log('Loading player details for:', playerName, 'context:', context);
    
    // Check cache first
    if (playerDetailsCache[playerName]) {
        console.log('Using cached data for:', playerName);
        contentEl.innerHTML = playerDetailsCache[playerName].html;
        contentEl.style.display = 'block';
        loadingEl.style.display = 'none';
        return playerDetailsCache[playerName].multiGraphData;
    }
    
    // Clear old content immediately
    contentEl.innerHTML = '';
    loadingEl.style.display = 'flex';
    contentEl.style.display = 'none';
    
    try {
        const response = await fetch(`/api/player-details/${encodeURIComponent(playerName)}`);
        const data = await response.json();
        
        if (data.success && data.tables.length > 0) {
            // Ensure player_name is set in data
            data.player_name = playerName;
            const html = renderPlayerDetails(data, context);
            contentEl.innerHTML = html;
            contentEl.style.display = 'block';
            
            // Cache the data
            playerDetailsCache[playerName] = {
                html: html,
                multiGraphData: data
            };
            
            return data;
        } else {
            contentEl.innerHTML = '<p class="no-data">No detailed data available for this player.</p>';
            contentEl.style.display = 'block';
        }
    } catch (error) {
        console.error('Error loading player details:', error);
        contentEl.innerHTML = `<p class="error">Failed to load player details: ${error.message}</p>`;
        contentEl.style.display = 'block';
    } finally {
        loadingEl.style.display = 'none';
    }
}

function renderPlayerDetails(data, context = 'modal') {
    // Filter out empty tables and tables we don't recognize
    const validTables = data.tables.filter(table => {
        if (!table.data || table.data.length === 0) return false;
        if (!table.columns || table.columns.length === 0) return false;
        
        // Only include recognized table types (skip generic "Table X" types)
        const columns = table.columns;
        const isRecognized = columns.includes('Killed by') || 
                           columns.includes('Victim') || 
                           columns.includes('Online time') || 
                           columns.includes('Raw XP no dia');
        return isRecognized;
    });
    
    if (validTables.length === 0) {
        return '<div class="no-data">No player details available for this player.</div>';
    }
    
    let html = '<div class="player-details-container">';
    
    html += '<div class="player-details-tabs">';
    
    validTables.forEach((table, index) => {
        const tableTitle = getTableTitle(table, index);
        const tabId = context === 'modal' ? `switchDetailTab(${index})` : `switchDashboardTab(${index})`;
        html += `<button class="detail-tab-btn${index === 0 ? ' active' : ''}" onclick="${tabId}">${tableTitle}</button>`;
    });
    
    html += '</div>';
    html += '<div class="player-details-tables">';
    
    validTables.forEach((table, index) => {
        html += `<div class="detail-table-container${index === 0 ? ' active' : ''}" id="${context}DetailTable${index}">`;
        
        // Table section
        html += '<div class="table-section">';
        html += renderTable(table);
        html += '</div>';
        
        // Chart section (if applicable)
        if (isXPTable(table)) {
            html += '<div class="chart-section-wrapper">';
            html += '<div class="chart-section">';
            html += '<h3>📊 Online Time vs EXP Analysis</h3>';
            const chartId = `${context}OnlineXpChart${index}`;
            html += `<div id="${chartId}" class="online-xp-chart"></div>`;
            html += '</div>';
            html += '</div>';
            setTimeout(() => renderOnlineXPChart(table, chartId), 100);
        }
        
        html += '</div>';
    });
    
    html += '</div>';
    html += '</div>';
    
    // Render combined overview for modal multi-graph tab
    if (context === 'modal') {
        setTimeout(() => renderCombinedOverview(data, 'modalMultiGraphDiv', data.player_name || null), 100);
    }
    // Render combined overview for dashboard
    if (context === 'dashboard') {
        setTimeout(() => renderCombinedOverview(data, 'dashboardMultiGraphDiv', data.player_name || null), 100);
    }
    
    return html;
}

function getTableTitle(table, index) {
    const columns = table.columns;
    if (columns.includes('Killed by')) return '💀 Deaths';
    if (columns.includes('Victim')) return '⚔️ Kills';
    if (columns.includes('Online time')) return '⏰ Online Time';
    if (columns.includes('Raw XP no dia')) return '📈 Daily XP';
    return `Table ${index + 1}`;
}

function isXPTable(table) {
    return table.columns.includes('Raw XP no dia') && table.columns.includes('Online time');
}

function renderTable(table) {
    if (!table || !table.columns || !table.data) {
        return '<div class="no-data">Invalid table data</div>';
    }
    
    let html = '<div class="detail-table-wrapper"><table class="detail-data-table">';
    html += '<thead><tr>';
    table.columns.forEach(col => {
        html += `<th>${col || 'N/A'}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    table.data.forEach(row => {
        html += '<tr>';
        row.forEach(cell => {
            // Handle undefined/null values
            const cellValue = cell !== null && cell !== undefined ? cell : '-';
            html += `<td>${cellValue}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table></div>';
    return html;
}

function renderOnlineXPChart(table, index) {
    // Extract online time and XP data
    const dateCol = table.columns.indexOf('Date') >= 0 ? 'Date' : table.columns.indexOf('Data') >= 0 ? 'Data' : null;
    const onlineCol = table.columns.indexOf('Online time');
    const xpCol = table.columns.indexOf('Raw XP no dia');
    
    if (dateCol === null || onlineCol < 0 || xpCol < 0) return;
    
    const dates = [];
    const onlineTimes = [];
    const xpValues = [];
    
    table.data.forEach(row => {
        dates.push(row[table.columns.indexOf(dateCol === 'Date' ? 'Date' : 'Data')]);
        
        // Parse online time (e.g., "2h 15m" to minutes)
        const onlineStr = row[onlineCol];
        const hours = onlineStr.match(/(\\d+)h/);
        const minutes = onlineStr.match(/(\\d+)m/);
        const totalMinutes = (hours ? parseInt(hours[1]) * 60 : 0) + (minutes ? parseInt(minutes[1]) : 0);
        onlineTimes.push(totalMinutes);
        
        // Parse XP (remove dots and convert)
        const xpStr = row[xpCol].replace(/\\./g, '');
        xpValues.push(parseInt(xpStr));
    });
    
    // Create dual-axis chart
    const trace1 = {
        x: dates,
        y: onlineTimes,
        name: 'Online Time (min)',
        type: 'bar',
        yaxis: 'y',
        marker: { color: 'rgba(147, 51, 234, 0.6)' }
    };
    
    const trace2 = {
        x: dates,
        y: xpValues,
        name: 'XP Gained',
        type: 'scatter',
        mode: 'lines+markers',
        yaxis: 'y2',
        line: { color: 'rgb(255, 127, 14)', width: 3 },
        marker: { size: 8 }
    };
    
    const layout = {
        title: 'Online Time vs XP Efficiency',
        xaxis: { title: 'Date', type: 'category', tickangle: -45 },
        yaxis: { title: 'Online Time (minutes)', side: 'left' },
        yaxis2: {
            title: 'XP Gained',
            overlaying: 'y',
            side: 'right'
        },
        height: 350,
        showlegend: true,
        template: 'plotly_white',
        margin: { l: 60, r: 60, t: 50, b: 100 }
    };
    
    Plotly.newPlot(chartId, [trace1, trace2], layout, {responsive: true});
}

function renderCombinedOverview(data, chartElementId = 'combinedOverviewChart', playerName = null) {
    try {
        if (!data || !data.tables || data.tables.length === 0) {
            document.getElementById(chartElementId).innerHTML = '<p class="no-data">No data available for overview</p>';
            return;
        }
        
        // Aggregate all data by datetime
        const timeSeriesData = new Map(); // key: datetime string, value: {rawXP, onlineMinutes, level, deaths, kills}
        
        // Process all tables
        data.tables.forEach(table => {
            // Skip table if it's missing, has no data, or has no columns
            if (!table || !table.data || table.data.length === 0 || !table.columns || table.columns.length === 0) return;
            
            const cols = table.columns;
        
        // Find date/time columns
        let dateColIdx = -1;
        let timeColIdx = -1;
        
        for (let i = 0; i < cols.length; i++) {
            // Skip if column is not a string
            if (typeof cols[i] !== 'string') continue;
            const col = cols[i].toLowerCase();
            if (col === 'date' || col === 'data') dateColIdx = i;
            if (col === 'time' || col === 'hora') timeColIdx = i;
        }
        
        // Process each row
        table.data.forEach(row => {
            if (dateColIdx < 0) return; // No date column, skip
            
            const dateStr = row[dateColIdx];
            let datetimeKey;
            
            if (timeColIdx >= 0 && row[timeColIdx]) {
                // Has time, use date + time
                datetimeKey = `${dateStr} ${row[timeColIdx]}`;
            } else {
                // No time, use just date (all times of that day will be aggregated)
                datetimeKey = dateStr;
            }
            
            // Initialize entry if doesn't exist
            if (!timeSeriesData.has(datetimeKey)) {
                timeSeriesData.set(datetimeKey, {
                    datetime: datetimeKey,
                    rawXP: 0,
                    onlineMinutes: 0,
                    level: 0,
                    deaths: 0,
                    kills: 0
                });
            }
            
            const entry = timeSeriesData.get(datetimeKey);
            
            // Parse data based on column names
            cols.forEach((colName, idx) => {
                // Skip if column name is not a string
                if (typeof colName !== 'string') return;
                
                const value = row[idx];
                const colLower = colName.toLowerCase();
                
                // Raw XP
                if (colLower.includes('raw xp') || colLower.includes('xp no dia')) {
                    const xpStr = String(value).replace(/\./g, '').replace(/,/g, '');
                    const xp = parseInt(xpStr) || 0;
                    entry.rawXP += xp;
                }
                
                // Online time
                if (colLower.includes('online time') || colLower.includes('tempo online')) {
                    const onlineStr = String(value);
                    const hours = onlineStr.match(/(\d+)h/);
                    const minutes = onlineStr.match(/(\d+)m/);
                    const totalMin = (hours ? parseInt(hours[1]) * 60 : 0) + (minutes ? parseInt(minutes[1]) : 0);
                    entry.onlineMinutes += totalMin;
                }
                
                // Level
                if (colLower === 'level' || colLower === 'nível') {
                    const level = parseInt(value) || 0;
                    entry.level = Math.max(entry.level, level); // Take max level
                }
                
                // Deaths
                if (colLower.includes('killed by') || colLower.includes('morto por')) {
                    entry.deaths++;
                }
                
                // Kills
                if (colLower === 'victim' || colLower === 'vítima') {
                    entry.kills++;
                }
            });
        });
    });
    
    if (timeSeriesData.size === 0) {
        document.getElementById(chartElementId).innerHTML = '<p class="no-data">No data available for overview</p>';
        return;
    }
    
    // Convert to arrays and sort by datetime
    const sortedEntries = Array.from(timeSeriesData.values()).sort((a, b) => {
        // Parse datetime for proper sorting
        const parseDateTime = (dtStr) => {
            // Try to parse various date formats
            // Format: "DD/MM/YYYY HH:MM:SS" or "DD/MM/YYYY"
            const parts = dtStr.split(' ');
            const datePart = parts[0];
            const timePart = parts[1] || '00:00:00';
            
            const dateParts = datePart.split('/');
            if (dateParts.length >= 3) {
                const day = dateParts[0];
                const month = dateParts[1];
                const year = dateParts[2];
                return new Date(`${year}-${month}-${day} ${timePart}`).getTime();
            }
            
            // Fallback to native parsing
            return new Date(dtStr).getTime();
        };
        
        return parseDateTime(a.datetime) - parseDateTime(b.datetime);
    });
    
    const datetimes = sortedEntries.map(e => e.datetime);
    const rawXPs = sortedEntries.map(e => e.rawXP);
    const onlineTimes = sortedEntries.map(e => e.onlineMinutes);
    const levels = sortedEntries.map(e => e.level);
    const deaths = sortedEntries.map(e => e.deaths);
    const kills = sortedEntries.map(e => e.kills);
    
    // Create traces
    const traces = [];
    
    // Raw XP (Line on Y1)
    if (rawXPs.some(v => v > 0)) {
        traces.push({
            x: datetimes,
            y: rawXPs,
            name: 'Raw XP',
            type: 'scatter',
            mode: 'lines+markers',
            yaxis: 'y1',
            line: { color: '#ff7f0e', width: 2 },
            marker: { size: 6 }
        });
    }
    
    // Online Time (Bar on Y2)
    if (onlineTimes.some(v => v > 0)) {
        traces.push({
            x: datetimes,
            y: onlineTimes,
            name: 'Online Time (min)',
            type: 'bar',
            yaxis: 'y2',
            marker: { color: 'rgba(147, 51, 234, 0.6)' }
        });
    }
    
    // Level (Line on Y3)
    if (levels.some(v => v > 0)) {
        traces.push({
            x: datetimes,
            y: levels,
            name: 'Level',
            type: 'scatter',
            mode: 'lines+markers',
            yaxis: 'y3',
            line: { color: '#00bcd4', width: 2 },
            marker: { size: 6 }
        });
    }
    
    // Deaths (Bar on Y4)
    if (deaths.some(v => v > 0)) {
        traces.push({
            x: datetimes,
            y: deaths,
            name: 'Deaths',
            type: 'bar',
            yaxis: 'y4',
            marker: { color: 'rgba(244, 67, 54, 0.6)' }
        });
    }
    
    // Kills (Bar on Y4)
    if (kills.some(v => v > 0)) {
        traces.push({
            x: datetimes,
            y: kills,
            name: 'Kills',
            type: 'bar',
            yaxis: 'y4',
            marker: { color: 'rgba(76, 175, 80, 0.6)' }
        });
    }
    
    if (traces.length === 0) {
        document.getElementById(chartElementId).innerHTML = '<p class="no-data">No data available for overview</p>';
        return;
    }
    
    const layout = {
        title: playerName ? `${playerName} - Player Overview` : 'Combined Player Overview',
        xaxis: {
            title: 'Date/Time',
            type: 'category',
            tickangle: -45
        },
        yaxis: {
            title: 'Raw XP',
            titlefont: { color: '#ff7f0e' },
            tickfont: { color: '#ff7f0e' }
        },
        yaxis2: {
            title: 'Online Time (min)',
            titlefont: { color: '#9333ea' },
            tickfont: { color: '#9333ea' },
            overlaying: 'y',
            side: 'right'
        },
        yaxis3: {
            title: 'Level',
            titlefont: { color: '#00bcd4' },
            tickfont: { color: '#00bcd4' },
            anchor: 'free',
            overlaying: 'y',
            side: 'left',
            position: 0.05
        },
        yaxis4: {
            title: 'Deaths/Kills',
            titlefont: { color: '#777' },
            tickfont: { color: '#777' },
            anchor: 'free',
            overlaying: 'y',
            side: 'right',
            position: 0.95
        },
        height: 500,
        showlegend: true,
        legend: { x: 0.5, y: 1.15, xanchor: 'center', orientation: 'h' },
        template: 'plotly_white',
        margin: { l: 80, r: 120, t: 100, b: 100 },
        barmode: 'group'
    };
    
    Plotly.newPlot(chartElementId, traces, layout, {responsive: true});
    } catch (error) {
        console.error('Error rendering combined overview:', error);
        const chartElement = document.getElementById(chartElementId);
        if (chartElement) {
            chartElement.innerHTML = '<p class="no-data">Failed to load data: ' + error.message + '</p>';
        }
    }
}

function switchDetailTab(tabIndex) {
    document.querySelectorAll('.detail-tab-btn').forEach((btn, i) => {
        btn.classList.toggle('active', i === tabIndex);
    });
    document.querySelectorAll('[id^="modalDetailTable"]').forEach((table, i) => {
        table.classList.toggle('active', i === tabIndex);
    });
}

function switchDashboardTab(tabIndex) {
    document.querySelectorAll('.detail-tab-btn').forEach((btn, i) => {
        btn.classList.toggle('active', i === tabIndex);
    });
    document.querySelectorAll('[id^="dashboardDetailTable"]').forEach((table, i) => {
        table.classList.toggle('active', i === tabIndex);
    });
}

function switchModalMainTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.modal-main-tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.modal-tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`modal-${tabName}`).classList.add('active');
    
    // If switching to multi-graph tab, ensure data is loaded and rendered
    if (tabName === 'multi-graph') {
        // Use data attribute instead of parsing title
        const modal = document.getElementById('playerModal');
        const playerName = modal.dataset.playerName || document.getElementById('modalPlayerName').textContent.split('(')[0].trim();
        
        // Check if multi-graph already has content
        const multiGraphDiv = document.getElementById('modalMultiGraphDiv');
        if (multiGraphDiv && !multiGraphDiv.hasChildNodes()) {
            // Data not yet rendered, trigger load
            loadPlayerDetailsForMultiGraph(playerName);
        }
    }
}

function switchDashboardMainTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.dashboard-main-tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.dashboard-tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`dashboard-${tabName}`).classList.add('active');
}

async function loadPlayerDetailsForMultiGraph(playerName) {
    const multiGraphDiv = document.getElementById('modalMultiGraphDiv');
    const multiGraphLoading = document.getElementById('multiGraphLoading');
    
    // Check cache first
    if (playerDetailsCache[playerName] && playerDetailsCache[playerName].multiGraphData) {
        const data = playerDetailsCache[playerName].multiGraphData;
        if (data.success && data.tables.length > 0) {
            renderCombinedOverview(data, 'modalMultiGraphDiv', playerName);
        }
        if (multiGraphLoading) multiGraphLoading.style.display = 'none';
        return;
    }
    
    if (multiGraphLoading) multiGraphLoading.style.display = 'flex';
    
    try {
        const response = await fetch(`/api/player-details/${encodeURIComponent(playerName)}`);
        const data = await response.json();
        
        // Cache the data
        if (!playerDetailsCache[playerName]) {
            playerDetailsCache[playerName] = {};
        }
        playerDetailsCache[playerName].multiGraphData = data;
        
        if (data.success && data.tables.length > 0) {
            renderCombinedOverview(data, 'modalMultiGraphDiv', playerName);
        } else {
            multiGraphDiv.innerHTML = '<p class="no-data">No data available for multi-graph</p>';
        }
    } catch (error) {
        multiGraphDiv.innerHTML = `<p class="error">Failed to load data: ${error.message}</p>`;
    } finally {
        if (multiGraphLoading) multiGraphLoading.style.display = 'none';
    }
}

window.switchDetailTab = switchDetailTab;
window.switchDashboardTab = switchDashboardTab;
window.switchModalMainTab = switchModalMainTab;
window.switchDashboardMainTab = switchDashboardMainTab;

function closePlayerModal() {
    const modal = document.getElementById('playerModal');
    modal.classList.remove('active');
    
    // Clear cache when modal closes to ensure fresh data next time
    const modalPlayerName = document.getElementById('modalPlayerName').textContent;
    const playerName = modalPlayerName.replace('📊 ', '').replace(' - EXP History', '').trim();
    if (playerName && playerDetailsCache[playerName]) {
        delete playerDetailsCache[playerName];
    }
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
        const params = new URLSearchParams({
            limit: 100,
            world: currentFilters.world,
            guild: currentFilters.guild
        });
        const response = await fetch(`/api/delta?${params}`);
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
