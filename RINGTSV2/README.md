# RINGTS V2 - Tibia Character Tracking System

**Simplified, Reliable, and Straightforward**

A clean rewrite of the Tibia character tracking system with focus on simplicity and reliability.

## 🎯 What's New in V2

### ✨ Improvements

1. **Simplified Code Structure**
   - Clear separation of concerns
   - Single responsibility per module
   - No code duplication

2. **Fixed Delta Calculations**
   - Delta inserts now work correctly
   - Proper column name mapping
   - Always calculates deltas when data changes

3. **Better Error Handling**
   - Context managers for database sessions
   - Automatic rollback on errors
   - Clear error messages

4. **Streamlined Functions**
   - Removed verbose prints
   - Single-purpose functions
   - Consistent naming conventions

5. **Maintained Database Structure**
   - Same schema as V1
   - All relationships preserved
   - Foreign keys intact

## 📁 Module Structure

```
RINGTSV2/
├── database.py          # Database models and manager
├── scraper.py          # Web scraping functions
├── parsers.py          # Data parsing utilities
├── data_processor.py   # Data insertion and delta calculation
├── analytics.py        # Analytics and reporting queries
├── main.py            # Main entry point with example
├── examples.py        # Comprehensive examples
└── README.md          # This file
```

## 🚀 Quick Start

### 1. Basic Usage

```python
from database import Database
from scraper import scrape_character, scrape_status
from data_processor import process_character

# Initialize database
db = Database('tibia.db')
db.create_tables()

# Scrape character data
tables = scrape_character("Rollabostx")
status_data = scrape_status()

# Process and store
results = process_character(db, "Rollabostx", "Auroria", tables, status_data)
```

### 2. Analytics

```python
from analytics import (
    get_top_xp_players,
    get_character_summary,
    get_top_xp_delta_players
)

# Top players by XP (last 7 days)
top_xp = get_top_xp_players(db, n=10, days=7)
print(top_xp)

# Character summary
summary = get_character_summary(db, "Rollabostx", days=7)
print(summary)

# Best XP rates (last 24 hours)
top_rates = get_top_xp_delta_players(db, n=10, hours=24)
print(top_rates)
```

### 3. Run Examples

```python
python examples.py
```

## 📊 Database Schema

### Core Tables
- **characters** - Character basic info
- **scraping_sessions** - Scraping metadata
- **character_deaths** - Death records
- **character_kills** - Kill records
- **character_online_times** - Daily online time
- **character_experiences** - Daily XP records

### Delta Tables
- **character_delta_experiences** - XP changes between sessions
- **character_delta_onlines** - Online time changes between sessions

## 🔧 Key Functions

### Data Processing
- `get_or_create_character()` - Get/create character
- `create_scraping_session()` - Create session record
- `insert_deaths()` - Insert death records
- `insert_kills()` - Insert kill records
- `insert_online_times()` - Insert online time records
- `insert_experiences()` - Insert experience records
- `calculate_experience_delta()` - Calculate XP deltas
- `calculate_online_delta()` - Calculate online deltas
- `process_character()` - Main processing function

### Analytics
- `get_top_xp_players()` - Top XP gainers
- `get_top_online_players()` - Most active players
- `get_top_killers()` - Most kills
- `get_character_summary()` - Character overview
- `get_top_xp_delta_players()` - Best XP rates
- `get_character_delta_summary()` - Recent activity
- `get_character_xp_history()` - Historical XP data
- `export_to_csv()` - Export to CSV

## 🐛 Issues Fixed from V1

### 1. Delta Inserts Not Running
**Problem**: Delta functions checked wrong column names
- Checked `'Raw XP/day'` instead of `'Raw XP no dia'`
- Checked `'Online Time'` instead of `'Online time'`

**Solution**: Created `normalize_column_names()` function that maps all variants

### 2. Verbose Output
**Problem**: Too many print statements cluttering output

**Solution**: Reduced prints to essentials, clear formatting

### 3. Code Duplication
**Problem**: Multiple versions of similar functions in advanced_analytics.py

**Solution**: Single implementation per function, no duplicates

### 4. Complex Logic
**Problem**: Overly complex parsing and processing logic

**Solution**: Simplified with clear, single-purpose functions

### 5. Error Handling
**Problem**: Inconsistent session management

**Solution**: Context managers for automatic cleanup

## 💡 Usage Tips

1. **Always scrape status data** for accurate delta timing
2. **Run periodically** to track deltas over time
3. **Use analytics functions** instead of raw SQL queries
4. **Export to CSV** for external analysis
5. **Check column names** if using custom scraped data

## 📈 Performance

- **Fast queries** with proper indexes
- **Efficient inserts** with unique constraints
- **Context managers** prevent connection leaks
- **Batch processing** supported

## 🔄 Migration from V1

The database structure is identical, so you can:

1. Point V2 to existing V1 database
2. Or start fresh with new database
3. No schema changes needed

```python
# Use existing V1 database
db = Database('tibia_scraper.db')  # Your V1 db file

# Everything works the same
```

## 🎓 Examples Included

Run `examples.py` to see:
1. Basic character scraping
2. Multiple character processing
3. Analytics queries
4. Delta tracking
5. CSV export

## 📝 Dependencies

```
sqlalchemy
pandas
requests
beautifulsoup4
```

Install with:
```bash
pip install sqlalchemy pandas requests beautifulsoup4
```

## ✅ Testing

```python
# Test database
python main.py

# Test all features
python examples.py
```

## 🏆 Summary

RINGTS V2 delivers:
- ✅ Working delta calculations
- ✅ Clean, simple code
- ✅ Same database structure
- ✅ Better error handling
- ✅ No code duplication
- ✅ Comprehensive examples
- ✅ Easy to understand and maintain

**Ready to use in production!**
