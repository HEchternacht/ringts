# RINGTS - Real-time Interactive Guild Tracking System

A real-time analytics dashboard for tracking player experience gains and performance in Rubinot. Built with Flask, Plotly, and modern web technologies.

![Python](https://img.shields.io/badge/python-3.12+-blue.svg)
![Flask](https://img.shields.io/badge/flask-3.1.2-green.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## 🎮 Features

### 📊 Interactive Visualizations
- **Real-time EXP tracking** - Monitor player experience gains over time
- **Interactive Plotly graphs** - Zoom, pan, and explore data dynamically
- **Multi-player comparison** - Compare up to multiple players simultaneously
- **Date range filtering** - Analyze specific time periods

### 🏆 Rankings & Statistics
- **Player rankings table** - Sortable, searchable rankings
- **Detailed statistics** - Total EXP, averages, min/max gains
- **Percentile rankings** - See where players stand
- **All-time and period-based** - Compare different timeframes

### 🔴 Live Feed
- **Real-time updates** - See EXP gains as they happen (polling every 1 minute)
- **Grouped by time** - Updates organized by collection time
- **Sorted by gain** - Highest gains displayed first
- **Click to view** - Click any player to see their individual graph

### 🖥️ System Console
- **Live logs** - Real-time server-sent events stream
- **Scraper status** - Monitor background data collection
- **Manual updates** - Trigger ranking updates on demand
- **Error tracking** - View system errors and warnings

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- pip or uv package manager

### Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd ringts
```

2. **Install dependencies**

Using pip:
```bash
pip install -r requirements.txt
```

Using uv (recommended):
```bash
uv sync
```

3. **Run the application**
```bash
python flask_app.py
```

4. **Open your browser**
```
http://localhost:5000
```

## 🐳 Docker Deployment

See [README-DEPLOYMENT.md](README-DEPLOYMENT.md) for detailed deployment instructions.

**Quick Deploy:**
```bash
docker-compose up -d
```

## 📖 Usage

### Dashboard Tab
1. **Select Players** - Search and click to add players to analysis
2. **Choose Date Range** (optional) - Filter by specific time period
3. **Generate Visualization** - Click to create interactive graphs
4. **View Statistics** - See detailed stats and rankings below graph

### Rankings Table Tab
1. **Select Date Range** - Choose period or leave empty for all-time
2. **Load Rankings** - View complete player rankings
3. **Search/Sort** - Use controls to filter and order results
4. **Click Player Names** - View individual player graphs

### Console Tab
- Monitor real-time system logs
- View scraper status and updates
- Track errors and debugging info

## 🏗️ Architecture

### Backend
- **Flask** - Web framework
- **Pandas** - Data manipulation and analysis
- **Plotly** - Interactive visualization generation
- **BeautifulSoup4** - Web scraping
- **httpx** - Async HTTP requests with proxy support

### Frontend
- **Vanilla JavaScript** - No framework overhead
- **Plotly.js** - Client-side rendering
- **Server-Sent Events** - Real-time console updates
- **Polling** - Delta updates every 1 minute

### Data Storage
- **CSV files** - Simple, portable data storage
- **File-based locking** - Thread-safe operations
- **Abstraction layer** - Easy migration to SQLite/PostgreSQL

## 📁 Project Structure

```
ringts/
├── flask_app.py           # Main Flask application
├── static/
│   ├── app.js            # Frontend JavaScript
│   └── style.css         # Styling
├── templates/
│   └── index.html        # Main HTML template
├── data/                 # CSV storage (auto-created)
│   ├── exps.csv         # Player experience data
│   └── deltas.csv       # Experience change records
├── Dockerfile            # Docker container definition
├── docker-compose.yml    # Docker Compose orchestration
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 🔧 Configuration

### Environment Variables
Copy `.env.example` to `.env` and customize:

```bash
FLASK_APP=flask_app.py
FLASK_ENV=production
HOST=0.0.0.0
PORT=5000
```

### Application Settings
Edit `flask_app.py` to customize:
- **Scraper interval** - How often to check for updates
- **Proxy list** - HTTP proxies for web scraping
- **World/Guild** - Target world and guild name
- **Data folder** - CSV storage location

## 🛠️ Development

### Running in Development Mode

```bash
# With debug enabled
python flask_app.py
```

### Code Structure

**Database Layer:**
- `Database` class - Abstraction for CSV storage
- `_read_exps()` / `_write_exps()` - EXP data operations
- `_read_deltas()` / `_write_deltas()` - Delta operations
- Thread-safe locking

**Scraper:**
- `get_ranking()` - Fetch guild rankings
- `return_last_update()` - Get latest update timestamp
- `loop_get_rankings()` - Background scraper loop
- Auto-restart on failure

**API Endpoints:**
- `GET /api/players` - List all players
- `POST /api/graph` - Generate visualization
- `GET /api/delta` - Polling endpoint for updates
- `POST /api/rankings-table` - Get rankings data
- `GET /api/console-stream` - SSE for logs
- `POST /api/manual-update` - Trigger update

## 🚨 Troubleshooting

### Port Already in Use
```bash
# Find process using port 5000
netstat -ano | findstr :5000  # Windows
lsof -i :5000                 # Linux/Mac

# Kill the process or change port in flask_app.py
```

### Scraper Not Working
- Check proxy list in `flask_app.py`
- Verify network connectivity
- Check console logs for errors
- Try manual update button

### Data Not Persisting
- Ensure `data/` folder exists
- Check file permissions
- Verify disk space

### High Memory Usage
- Reduce worker count in Gunicorn
- Limit delta history in database
- Consider database migration to SQLite

## 📊 Performance

### Optimization Tips
1. **Database Migration** - Switch to SQLite for better performance with large datasets
2. **Caching** - Implement Redis for frequently accessed data
3. **Background Workers** - Separate scraper from web server
4. **Load Balancing** - Use nginx for multiple instances

### Current Limitations
- CSV file I/O on every request (consider SQLite)
- Single-threaded scraper
- In-memory delta queue (not persistent across restarts)
- No authentication/authorization

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Rubinot community for inspiration
- Flask and Plotly teams for excellent frameworks
- All contributors and testers

## 📧 Contact

For questions, issues, or suggestions, please open an issue on GitHub.

---

**Built with ❤️ for the Rubinot community**


```python
# Simple SQLite setup - just specify a database file path
db_manager = get_database_manager('tibia_scraper.db')  # Creates tibia_scraper.db file
```

### 4. Create Database Tables

```python
from alchemy import get_database_manager

# Simple SQLite setup
db_manager = get_database_manager('tibia_scraper.db')  # Creates the database file
db_manager.create_tables()  # Run this once to create all tables
```

## Usage Examples

### Basic Character Scraping and Storage

```python
from alchemy import get_database_manager
from utils import process_character_data

# Initialize SQLite database (no server required!)
db_manager = get_database_manager('tibia_scraper.db')  # Creates tibia_scraper.db file

# Scrape character data (using your existing functions)
character_name = "rollabostx"
scraped_data = scrape_player_data(character_name)  # Your existing function
status_data = get_last_status_updates()            # Your existing function

# Process and store only new data
if scraped_data['success']:
    results = process_character_data(
        db_manager=db_manager,
        character_name=character_name,
        world="Auroria",
        scraped_data=scraped_data['tables'],
        status_data=status_data
    )
    print(f"New records inserted: {results}")
```

### Analytics and Reporting

```python
from analytics import print_character_report, calculate_xp_growth

# Get comprehensive character report
print_character_report(db_manager, "rollabostx")

# Calculate specific metrics
xp_stats = calculate_xp_growth(db_manager, character_id=1, days_back=30)
print(f"30-day XP growth: {xp_stats['total_xp']:,}")
```

### Bulk Processing Multiple Characters

```python
def process_multiple_characters(character_list):
    for character_name in character_list:
        scraped_data = scrape_player_data(character_name)
        if scraped_data['success']:
            status_data = get_last_status_updates()
            results = process_character_data(
                db_manager, character_name, "Auroria", 
                scraped_data['tables'], status_data
            )
            print(f"{character_name}: {sum(results.values())} new records")

# Process multiple characters
characters = ["rollabostx", "character2", "character3"]
process_multiple_characters(characters)
```

## Key Features Explained

### 1. Differential Data Storage
- Only new deaths, kills, online times, and experiences are stored
- System compares timestamps/dates with existing data
- No duplicate entries, ever

### 2. Scraping Session Tracking  
- Each scraping run creates a session record
- Tracks status updates from rubinothings.com.br
- Links all data to the scraping session for full traceability

### 3. Data Parsing & Cleaning
- Converts "3h 10m" to minutes automatically
- Parses "176.495.455" XP strings to integers
- Handles Brazilian date format (DD/MM/YYYY)
- Cleans level delta strings ("+2", "-1")

### 4. Analytics Functions
- XP growth calculations over any period
- Death/kill rate analysis
- Online time statistics
- Character progression tracking

## Data Flow

1. **Scraping** → Your existing functions get raw data
2. **Session Creation** → Creates scraping session with timestamps
3. **Character Management** → Creates or finds existing character
4. **Data Comparison** → Compares new data with existing records
5. **Differential Insert** → Inserts only new/changed records
6. **Analytics** → Calculate statistics and trends

## Monitoring & Maintenance

- Check scraping session status to monitor data freshness
- Use analytics functions to identify data quality issues
- Export data to CSV for external analysis
- Monitor database growth and performance

## Example Analytics Output

```
📊 CHARACTER REPORT: rollabostx
==================================================
🌍 World: Auroria
🆔 Character ID: 1
📅 First Tracked: 2025-12-29
🔄 Last Updated: 2025-12-29

📈 30-DAY SUMMARY
------------------------------
💫 XP Growth: 2,643,533 XP (+12 levels)
📊 Daily Avg XP: 176,236
📏 Level: 1519 → 1531
💀 Deaths: 5 (0.17/day)
🗡️ Most Common Killer: frostreaper
⚔️ Kills: 10 (0.33/day)
🎯 Avg Victim Level: 904
🕐 Online Time: 164.2h (5.5h/day)
📅 Days Played: 4/30
```

## Troubleshooting

### Common Issues

1. **SQLite Database Locked**
   - Close any database browser tools that might have the file open
   - Make sure no other processes are using the database file
   - Restart your application if needed

2. **No New Records Inserted**
   - This is normal! System only stores differences
   - Check if character was recently scraped

3. **Date Parsing Issues**
   - System handles DD/MM/YYYY format automatically
   - Check for unusual date formats in source data

4. **Performance Issues**
   - SQLite is very fast for most use cases
   - Consider using WAL mode for better concurrent access
   - Use database indexes (already included) for large datasets

## Contributing

Feel free to extend the system with:
- Additional analytics functions
- More sophisticated data visualization
- Performance optimizations
- Support for other Tibia tracking websites

## License

This project is for educational and personal use. Respect the terms of service of any websites you scrape.