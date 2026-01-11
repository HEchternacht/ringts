# SQLite Migration Guide

This project now supports SQLite database using SQLAlchemy instead of CSV files for better performance and data integrity.

## Files Created

1. **database_models.py** - SQLAlchemy models and database manager
2. **database_sqlalchemy.py** - New Database class using SQLAlchemy (drop-in replacement)
3. **migrate_to_sqlite.py** - Migration script to convert CSV data to SQLite

## Migration Steps

### Step 1: Install SQLAlchemy

```bash
pip install sqlalchemy==2.0.25
```

Or if using requirements:
```bash
pip install -r requirements-fastapi.txt
```

### Step 2: Backup Your Data

```bash
# Create a backup of your current CSV files
cp var/data/exps.csv var/data/exps.csv.backup
cp var/data/deltas.csv var/data/deltas.csv.backup
cp var/data/vipsdata.csv var/data/vipsdata.csv.backup
cp var/data/deltavip.csv var/data/deltavip.csv.backup
```

### Step 3: Run Migration Script

```bash
python migrate_to_sqlite.py
```

This will:
- Read all CSV files
- Create a new SQLite database at `var/data/ringts.db`
- Migrate all data to the new database
- Preserve all existing data

### Step 4: Update fastapi_app.py

Replace the Database class import in fastapi_app.py:

**Find:**
```python
class Database:
    """Database abstraction layer for storing player EXP data."""
    # ... existing CSV-based implementation
```

**Replace with:**
```python
from database_sqlalchemy import SQLAlchemyDatabase as Database
```

Or manually replace the entire `Database` class with the one from `database_sqlalchemy.py`.

### Step 5: Test the Application

```bash
python fastapi_app.py
```

Verify that:
- All player data is visible
- Deltas are being recorded
- VIP tracking works
- No errors in the console

### Step 6: Clean Up (Optional)

Once you've verified everything works, you can optionally remove the old CSV files:

```bash
# Keep backups, remove originals
rm var/data/exps.csv
rm var/data/deltas.csv
rm var/data/vipsdata.csv
rm var/data/deltavip.csv
```

Note: Keep the backup files for at least a week to ensure everything is working correctly.

## Benefits of SQLite

1. **Performance**: Much faster queries, especially with large datasets
2. **Data Integrity**: ACID compliance, no data corruption
3. **Concurrent Access**: Better handling of concurrent reads/writes
4. **Smaller Size**: More efficient storage
5. **Indexes**: Faster lookups by player name, time, world, etc.
6. **No Duplicates**: Unique constraints prevent duplicate entries

## Rollback

If you need to rollback to CSV:

1. Stop the application
2. Restore the backup CSV files
3. Revert the Database class changes in fastapi_app.py
4. Restart the application

## Database Structure

The SQLite database contains these tables:

- **players** - Player experience data (replaces exps.csv)
- **deltas** - Experience deltas (replaces deltas.csv)
- **vips** - VIP player list (replaces vips.txt)
- **vip_data** - VIP daily data (replaces vipsdata.csv)
- **vip_deltas** - VIP deltas (replaces deltavip.csv)
- **status_data** - System status (still uses JSON file)
- **scraping_config** - Scraping configuration (moves from JSON to DB)

## Database Location

The SQLite database file is stored at:
```
var/data/ringts.db
```

You can backup this single file instead of multiple CSV files.

## Viewing the Database

You can use any SQLite browser to view/query the database:

- [DB Browser for SQLite](https://sqlitebrowser.org/) (Free, GUI)
- `sqlite3` command-line tool
- VS Code SQLite extensions

Example query:
```sql
SELECT name, exp, world, guild FROM players ORDER BY exp DESC LIMIT 10;
```
