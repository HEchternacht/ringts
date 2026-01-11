# Daily Reset Feature

The SQLAlchemy database now includes an automatic daily reset feature that truncates the players (exps) table at a configured time each day.

## Features

✅ **Timezone Aware** - Respects your server's timezone offset  
✅ **Resilient** - Survives server restarts without duplicate resets  
✅ **Once Per Day** - Guaranteed to run only once per day  
✅ **Persistent** - Tracks last reset date on disk  
✅ **Automatic** - Runs automatically in the scraper loop  

## Configuration

Set these environment variables:

```bash
# Time when daily reset should occur (24-hour format)
DAILY_RESET_HOUR=10        # Default: 10
DAILY_RESET_MINUTE=5       # Default: 5

# Timezone offset from UTC
TIMEZONE_OFFSET_HOURS=3    # Default: 3 (UTC+3)
```

## How It Works

1. **On Application Start**: Checks if reset is needed and performs it if due
2. **During Scraper Loop**: Checks every cycle if reset time has passed
3. **Persistence**: Records reset date in `var/data/last_reset.txt`
4. **Prevention**: Won't reset twice on the same calendar day

## Reset Logic

The reset will occur when:
- Current local time >= Reset time (e.g., 10:05)
- Last reset date != Today's date
- Reset hasn't been done yet today

## Example Scenarios

### Scenario 1: Normal Operation
```
09:00 - Application running, no reset needed
10:05 - Reset triggered automatically
10:06 - Reset complete, won't trigger again today
23:59 - Day ends
00:01 - Next day starts, reset will trigger at 10:05
```

### Scenario 2: Server Restart
```
09:00 - Server starts, no reset needed
10:05 - Reset performed
12:00 - Server crashes/restarts
12:01 - Server starts again, checks last_reset.txt
12:02 - Sees reset already done today, skips
```

### Scenario 3: Downtime During Reset Time
```
09:00 - Server running normally
10:00 - Server goes down for maintenance
11:00 - Server comes back up
11:01 - Checks and sees current time (11:01) > reset time (10:05)
11:02 - Sees no reset done today, performs reset
```

## Manual Testing

Run the test script to verify functionality:

```bash
python test_daily_reset.py
```

This will:
- Create a test database
- Add test players
- Check reset logic
- Verify persistence
- Test duplicate prevention

## Manual Reset

If you need to manually trigger a reset:

```python
from database_sqlalchemy import SQLAlchemyDatabase

db = SQLAlchemyDatabase()
db._perform_daily_reset()
```

## Monitoring

Check the last reset date:

```bash
cat var/data/last_reset.txt
```

Example output: `2026-01-11`

## Logs

Reset operations are logged with `[RESET]` prefix:

```
[RESET] Performing daily reset at 2026-01-11 10:05:00
[RESET] ✓ Daily reset complete - truncated 150 players
```

## Technical Details

### Files Modified
- `database_sqlalchemy.py` - Added reset methods
- `fastapi_app.py` - Integrated reset check into scraper loop
- `requirements-fastapi.txt` - Added pytz dependency

### Methods Added
- `_get_local_datetime()` - Get timezone-adjusted current time
- `_get_last_reset_date()` - Read last reset from file
- `_save_last_reset_date()` - Save reset date to file
- `_should_reset_today()` - Check if reset is due
- `_perform_daily_reset()` - Execute the reset
- `_check_and_perform_daily_reset()` - Check and reset on init
- `check_daily_reset()` - Public method for periodic checks

### Database Changes
The reset operation executes:
```sql
DELETE FROM players;
```

This clears all player EXP data while preserving:
- Historical deltas (for analytics)
- VIP tracking data
- Configuration data

## Troubleshooting

### Reset Not Happening

1. Check current local time:
   ```python
   from database_sqlalchemy import SQLAlchemyDatabase
   db = SQLAlchemyDatabase()
   print(db._get_local_datetime())
   ```

2. Check if reset is due:
   ```python
   print(db._should_reset_today())
   ```

3. Check last reset date:
   ```bash
   cat var/data/last_reset.txt
   ```

### Reset Happening Multiple Times

This shouldn't happen, but if it does:
1. Check the `last_reset.txt` file is being written correctly
2. Verify timezone configuration is correct
3. Check server clock/time

### Wrong Time

Adjust `TIMEZONE_OFFSET_HOURS` to match your timezone:
- UTC+0: `TIMEZONE_OFFSET_HOURS=0`
- UTC+3: `TIMEZONE_OFFSET_HOURS=3`
- UTC-5: `TIMEZONE_OFFSET_HOURS=-5`

## Migration from CSV

If migrating from the old CSV-based system, the reset functionality will automatically integrate once you switch to `SQLAlchemyDatabase`.

No additional migration steps required!
