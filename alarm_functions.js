// ============================================
// ALARM SYSTEM FUNCTIONS
// ============================================

// Initialize alarm audio
function initializeAlarmAudio() {
    // Create an audio context for the alarm sound
    alarmAudio = {
        context: null,
        play: function() {
            try {
                if (!this.context) {
                    this.context = new (window.AudioContext || window.webkitAudioContext)();
                }
                
                const oscillator = this.context.createOscillator();
                const gainNode = this.context.createGain();
                
                oscillator.connect(gainNode);
                gainNode.connect(this.context.destination);
                
                oscillator.frequency.value = 800;
                oscillator.type = 'sine';
                
                gainNode.gain.setValueAtTime(0.3, this.context.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, this.context.currentTime + 0.5);
                
                oscillator.start(this.context.currentTime);
                oscillator.stop(this.context.currentTime + 0.5);
                
                // Play multiple beeps
                setTimeout(() => {
                    const osc2 = this.context.createOscillator();
                    const gain2 = this.context.createGain();
                    osc2.connect(gain2);
                    gain2.connect(this.context.destination);
                    osc2.frequency.value = 1000;
                    osc2.type = 'sine';
                    gain2.gain.setValueAtTime(0.3, this.context.currentTime);
                    gain2.gain.exponentialRampToValueAtTime(0.01, this.context.currentTime + 0.5);
                    osc2.start(this.context.currentTime);
                    osc2.stop(this.context.currentTime + 0.5);
                }, 200);
                
                setTimeout(() => {
                    const osc3 = this.context.createOscillator();
                    const gain3 = this.context.createGain();
                    osc3.connect(gain3);
                    gain3.connect(this.context.destination);
                    osc3.frequency.value = 800;
                    osc3.type = 'sine';
                    gain3.gain.setValueAtTime(0.3, this.context.currentTime);
                    gain3.gain.exponentialRampToValueAtTime(0.01, this.context.currentTime + 0.5);
                    osc3.start(this.context.currentTime);
                    osc3.stop(this.context.currentTime + 0.5);
                }, 400);
            } catch (error) {
                console.error('Error playing alarm sound:', error);
            }
        }
    };
}

// Toggle alarm panel visibility
function toggleAlarmPanel() {
    const panel = document.getElementById('alarmPanel');
    const btn = document.getElementById('alarmToggleBtn');
    
    if (panel.style.display === 'none') {
        panel.style.display = 'block';
        btn.classList.add('active');
    } else {
        panel.style.display = 'none';
        btn.classList.remove('active');
    }
}

// Load alarm configuration from localStorage
function loadAlarmConfig() {
    try {
        const saved = localStorage.getItem('expAlarmConfig');
        if (saved) {
            const config = JSON.parse(saved);
            alarmConfig.guilds = config.guilds || [];
            alarmConfig.threshold = config.threshold || 1000000;
            alarmConfig.timeWindow = config.timeWindow || 5;
            alarmConfig.enabled = config.enabled || false;
            
            // Update UI
            document.getElementById('alarmGuilds').value = alarmConfig.guilds.join(', ');
            document.getElementById('alarmThreshold').value = alarmConfig.threshold;
            document.getElementById('alarmTimeWindow').value = alarmConfig.timeWindow;
            
            updateAlarmStatus();
        }
    } catch (error) {
        console.error('Error loading alarm config:', error);
    }
}

// Save alarm configuration
function saveAlarmConfig() {
    const guildsInput = document.getElementById('alarmGuilds').value;
    const threshold = parseInt(document.getElementById('alarmThreshold').value) || 1000000;
    const timeWindow = parseInt(document.getElementById('alarmTimeWindow').value) || 5;
    
    // Parse guilds (comma-separated)
    const guilds = guildsInput.split(',').map(g => g.trim()).filter(g => g.length > 0);
    
    if (guilds.length === 0) {
        showNotification('⚠️ Please enter at least one guild name', 'warning');
        return;
    }
    
    alarmConfig.guilds = guilds;
    alarmConfig.threshold = threshold;
    alarmConfig.timeWindow = timeWindow;
    alarmConfig.enabled = true;
    alarmConfig.snoozedUntil = null;
    
    // Save to localStorage
    localStorage.setItem('expAlarmConfig', JSON.stringify(alarmConfig));
    
    updateAlarmStatus();
    showNotification('✅ Alarm configuration saved!', 'success');
}

// Clear alarms
function clearAlarms() {
    alarmConfig.guilds = [];
    alarmConfig.enabled = false;
    alarmConfig.snoozedUntil = null;
    guildExpTracking.clear();
    
    document.getElementById('alarmGuilds').value = '';
    localStorage.removeItem('expAlarmConfig');
    
    updateAlarmStatus();
    showNotification('🗑️ Alarms cleared', 'info');
}

// Update alarm status indicator
function updateAlarmStatus() {
    const statusDiv = document.getElementById('alarmStatus');
    const indicator = statusDiv.querySelector('.alarm-indicator');
    const text = statusDiv.querySelector('span:last-child');
    
    if (alarmConfig.enabled && alarmConfig.guilds.length > 0) {
        indicator.className = 'alarm-indicator active';
        text.textContent = `Alarm: Active (${alarmConfig.guilds.length} guild${alarmConfig.guilds.length > 1 ? 's' : ''})`;
    } else {
        indicator.className = 'alarm-indicator inactive';
        text.textContent = 'Alarm: Inactive';
    }
}

// Test alarm (visual and sound)
function testAlarm() {
    triggerAlarm('Test Guild', 1234567, 5, true);
}

// Check if any monitored guild exceeded threshold
function checkGuildAlarms(delta) {
    if (!alarmConfig.enabled || alarmConfig.guilds.length === 0) {
        return;
    }
    
    // Check if snoozed
    if (alarmConfig.snoozedUntil && Date.now() < alarmConfig.snoozedUntil) {
        return;
    }
    
    // Check if this player belongs to a monitored guild
    // We need to extract guild from player name or use additional data
    // For now, we'll track by guild name pattern matching
    const playerName = delta.name;
    let matchedGuild = null;
    
    for (const guild of alarmConfig.guilds) {
        if (playerName.toLowerCase().includes(guild.toLowerCase())) {
            matchedGuild = guild;
            break;
        }
    }
    
    if (!matchedGuild) {
        return; // Player not in monitored guilds
    }
    
    // Track exp for this guild
    const now = Date.now();
    const cutoffTime = now - (alarmConfig.timeWindow * 60 * 1000);
    
    if (!guildExpTracking.has(matchedGuild)) {
        guildExpTracking.set(matchedGuild, []);
    }
    
    const guildData = guildExpTracking.get(matchedGuild);
    
    // Add this delta
    guildData.push({
        exp: delta.deltaexp,
        timestamp: now,
        player: playerName
    });
    
    // Remove old entries
    const filtered = guildData.filter(entry => entry.timestamp >= cutoffTime);
    guildExpTracking.set(matchedGuild, filtered);
    
    // Calculate total exp in time window
    const totalExp = filtered.reduce((sum, entry) => sum + entry.exp, 0);
    
    // Check if threshold exceeded
    if (totalExp >= alarmConfig.threshold) {
        triggerAlarm(matchedGuild, totalExp, alarmConfig.timeWindow, false);
        // Clear tracking to avoid repeated alarms
        guildExpTracking.set(matchedGuild, []);
    }
}

// Trigger the alarm popup
function triggerAlarm(guildName, totalExp, timeWindow, isTest) {
    const popup = document.getElementById('alarmPopup');
    const message = document.getElementById('alarmMessage');
    const details = document.getElementById('alarmDetails');
    
    const expFormatted = totalExp.toLocaleString();
    const thresholdFormatted = alarmConfig.threshold.toLocaleString();
    
    if (isTest) {
        message.textContent = `🧪 TEST ALARM`;
        details.innerHTML = `
            <p><strong>Guild:</strong> ${guildName}</p>
            <p><strong>EXP Gained:</strong> ${expFormatted}</p>
            <p><strong>Time Window:</strong> ${timeWindow} minutes</p>
            <p><em>This is a test alarm.</em></p>
        `;
    } else {
        message.textContent = `Guild "${guildName}" exceeded threshold!`;
        details.innerHTML = `
            <p><strong>Guild:</strong> ${guildName}</p>
            <p><strong>EXP Gained:</strong> ${expFormatted}</p>
            <p><strong>Threshold:</strong> ${thresholdFormatted}</p>
            <p><strong>Time Window:</strong> ${timeWindow} minutes</p>
            <p class="alarm-warning">⚠️ ${((totalExp / alarmConfig.threshold) * 100).toFixed(1)}% of threshold</p>
        `;
    }
    
    // Show popup with animation
    popup.style.display = 'flex';
    setTimeout(() => {
        popup.classList.add('show');
    }, 10);
    
    // Play alarm sound
    if (alarmAudio) {
        alarmAudio.play();
    }
    
    // Flash the page title
    flashPageTitle(guildName);
}

// Flash page title to get attention
let titleFlashInterval = null;
function flashPageTitle(guildName) {
    if (titleFlashInterval) {
        clearInterval(titleFlashInterval);
    }
    
    const originalTitle = document.title;
    let isAlert = false;
    let flashCount = 0;
    
    titleFlashInterval = setInterval(() => {
        document.title = isAlert ? originalTitle : `🚨 ALERT: ${guildName}!`;
        isAlert = !isAlert;
        flashCount++;
        
        if (flashCount >= 20) { // Flash for 10 seconds
            clearInterval(titleFlashInterval);
            document.title = originalTitle;
            titleFlashInterval = null;
        }
    }, 500);
}

// Dismiss alarm
function dismissAlarm() {
    const popup = document.getElementById('alarmPopup');
    popup.classList.remove('show');
    
    setTimeout(() => {
        popup.style.display = 'none';
    }, 300);
    
    if (titleFlashInterval) {
        clearInterval(titleFlashInterval);
        document.title = 'XAD - XISNOVE Analytics Dashboard';
        titleFlashInterval = null;
    }
}

// Snooze alarm for 5 minutes
function snoozeAlarm() {
    alarmConfig.snoozedUntil = Date.now() + (5 * 60 * 1000);
    dismissAlarm();
    showNotification('😴 Alarm snoozed for 5 minutes', 'info');
}
