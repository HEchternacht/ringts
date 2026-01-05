# Tibia Character Data Scraper & Database System

A comprehensive system for scraping, storing, and analyzing Tibia character data from rubinothings.com.br with SQLite database backend.

## Features

- 🎯 **Smart Data Collection**: Only stores new/changed data (no duplicates)
- 📊 **Complete Tracking**: Deaths, kills, online time, and experience progression
- ⏱️ **Scraping Session Tracking**: Tracks scraping times and status updates
- 📈 **Analytics**: Calculate XP growth, death rates, kill rates, and online time statistics
- 🚀 **Bulk Processing**: Process multiple characters efficiently
- 📄 **Data Export**: Export character data to CSV files
- 💾 **SQLite Backend**: No database server required, single file database

## Database Schema

The system uses 6 main tables:

1. **characters** - Main character information
2. **scraping_sessions** - Tracks each scraping run with timestamps
3. **character_deaths** - Death records with timestamps and details
4. **character_kills** - Kill records with victim information
5. **character_online_times** - Daily online time tracking
6. **character_experiences** - Daily XP and level progression

## Files Structure

- `alchemy.py` - SQLAlchemy database models and connection management
- `utils.py` - Data processing and insertion functions
- `analytics.py` - Analysis and reporting functions
- `example_usage.py` - Usage examples and testing
- `tet.ipynb` - Main scraping notebook with database integration
- `requirements.txt` - Python package dependencies

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup SQLite Database

SQLite is built into Python, so no additional database server installation is required! The system will automatically create a database file when you first run it.

### 3. Configure Database Connection

Update the database connection in your code (much simpler with SQLite):

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