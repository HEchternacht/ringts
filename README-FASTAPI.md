# FastAPI Migration - Ring Tracker

This document describes the FastAPI conversion of the Flask application.

## New File Structure

### Core Application Files

- **`fastapi_app.py`** - Main FastAPI application with all HTTP routes
- **`fastapi_database.py`** - Database abstraction layer (CSV-based storage)
- **`fastapi_utils.py`** - Utility functions (logging, parsing, proxy handling)
- **`fastapi_models.py`** - Pydantic models for request/response validation
- **`fastapi_scraper.py`** - Web scraping functions for game data
- **`fastapi_vip.py`** - VIP player tracking functions
- **`fastapi_analytics.py`** - Analytics and graph generation
- **`fastapi_background.py`** - Background task runner for continuous scraping

## Key Differences from Flask

### 1. **Async Support**
FastAPI is built on async/await, providing better performance for I/O operations.

### 2. **Automatic API Documentation**
- Swagger UI: `http://localhost:5000/docs`
- ReDoc: `http://localhost:5000/redoc`

### 3. **Request/Response Models**
All endpoints use Pydantic models for validation:
```python
from fastapi_models import GraphRequest, VIPAddRequest
```

### 4. **Dependency Injection**
Database and configuration can be injected into routes (though currently using global instance for simplicity).

### 5. **Type Hints**
Full type hints throughout the codebase for better IDE support and validation.

## Running the Application

### Install Dependencies
```bash
pip install fastapi uvicorn pandas plotly beautifulsoup4 requests httpx pebble python-multipart jinja2
```

### Run with Uvicorn
```bash
# Development mode with auto-reload
uvicorn fastapi_app:app --reload --host 0.0.0.0 --port 5000

# Production mode
uvicorn fastapi_app:app --host 0.0.0.0 --port 5000 --workers 4
```

### Environment Variables
Same as Flask version:
```bash
UPLOAD_PASSWORD=Rollabostx1234
DEFAULT_WORLD=Auroria
DEFAULT_GUILD=Ascended Auroria
DATA_FOLDER=/var/data
TIMEZONE_OFFSET_HOURS=3
DAILY_RESET_HOUR=10
DAILY_RESET_MINUTE=2
FORCE_PROXY=false
```

## API Endpoints

### Player Data
- `GET /api/players` - List all players
- `GET /api/date-range` - Get available date range
- `POST /api/graph` - Generate player graph
- `POST /api/stats` - Get player statistics
- `GET /api/top-players` - Get top players by EXP
- `GET /api/recent-updates` - Get recent EXP updates
- `POST /api/rankings-table` - Get rankings table
- `GET /api/player-graph/{player_name}` - Individual player graph
- `GET /api/player-details/{player_name}` - Detailed player data
- `GET /api/delta` - Get recent delta updates

### VIP Tracking
- `GET /api/vip/list` - List VIP players
- `POST /api/vip/add` - Add VIP player
- `POST /api/vip/remove` - Remove VIP player
- `GET /api/vip/deltas` - Get VIP delta history
- `POST /api/vip/graph` - Generate VIP graph

### System
- `GET /api/scraper-status` - Scraper status
- `GET /api/status-data` - World status data
- `GET /api/scraping-config` - Get scraping config
- `POST /api/scraping-config` - Update scraping config
- `POST /api/manual-update` - Trigger manual update
- `GET /api/console-stream` - SSE console stream
- `GET /health` - Health check

### File Management
- `GET /api/download/deltas` - Download deltas CSV
- `GET /api/download/exps` - Download exps CSV
- `POST /api/upload/deltas` - Upload deltas CSV
- `POST /api/upload/exps` - Upload exps CSV

## Module Organization

### fastapi_models.py
Pydantic models for:
- Request validation (GraphRequest, StatsRequest, VIPAddRequest, etc.)
- Response schemas (PlayerStats, DeltaUpdate, ScraperStatus, etc.)
- Error responses

### fastapi_database.py
`Database` class with methods:
- CSV file operations (read/write exps and deltas)
- VIP tracking (add/remove/update VIPs)
- Daily reset logic
- Status data management
- Scraping configuration

### fastapi_utils.py
Utility functions:
- `log_console()` - Logging with queue for SSE stream
- `parse_datetime()` - Parse Brazilian datetime format
- `parse_online_time_to_minutes()` - Parse time strings
- `get_multiple()` - Multi-proxy request handler

### fastapi_scraper.py
Web scraping:
- `extract_tables()` - Parse HTML tables
- `scrape_player_data()` - Scrape individual player
- `get_ranking()` - Get guild rankings
- `get_last_status_updates()` - Get world status
- `return_last_update()` - Get last update time
- `parse_to_db_formatted()` - Format ranking data

### fastapi_vip.py
VIP tracking:
- `scrape_single_vip()` - Scrape one VIP player
- `scrape_vip_data()` - Scrape all VIPs for a world
- `process_vip_deltas()` - Process VIP updates

### fastapi_analytics.py
Analytics:
- `get_delta_between()` - Filter deltas by date range
- `preprocess_vis_data()` - Compress zero periods
- `create_interactive_graph()` - Generate Plotly graphs
- `get_player_stats()` - Calculate statistics

### fastapi_background.py
Background tasks:
- `loop_get_rankings()` - Main scraping loop
- `start_scraper_thread()` - Start background thread
- `get_scraper_status()` - Get current status

## Migration Notes

1. **Templates remain unchanged** - Still using Jinja2 templates from `templates/`
2. **Static files remain unchanged** - Mounted from `static/`
3. **Data format unchanged** - CSV files compatible with Flask version
4. **Background scraper** - Runs in daemon thread (same as Flask)
5. **Queue-based logging** - Console logs streamed via SSE

## Testing

Test key endpoints:
```bash
# Health check
curl http://localhost:5000/health

# Get players
curl http://localhost:5000/api/players

# Get scraper status
curl http://localhost:5000/api/scraper-status

# Generate graph (POST with JSON)
curl -X POST http://localhost:5000/api/graph \
  -H "Content-Type: application/json" \
  -d '{"names": ["Player1", "Player2"]}'
```

## Docker Support

The FastAPI app can run with the existing Dockerfile, just change the command:

```dockerfile
# Old Flask command
CMD ["python", "flask_app.py"]

# New FastAPI command
CMD ["uvicorn", "fastapi_app:app", "--host", "0.0.0.0", "--port", "5000"]
```

## Performance Benefits

1. **Async I/O** - Better handling of concurrent requests
2. **Automatic documentation** - Built-in Swagger/ReDoc
3. **Data validation** - Pydantic models catch errors early
4. **Type safety** - Better IDE support and fewer runtime errors
5. **Modern Python** - Uses latest async/await patterns

## Backward Compatibility

The FastAPI version maintains the same:
- API endpoints (URL paths)
- Request/response formats
- Database schema (CSV files)
- Configuration (environment variables)
- Templates and static files

Frontend code should work without modification!
