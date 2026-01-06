# Migration from Threading to Multiprocessing

## Overview
The background scraper has been migrated from using `threading.Thread` to `multiprocessing.Process` for improved performance and reliability.

## Benefits

### 1. **No GIL Limitations**
- Python's Global Interpreter Lock (GIL) prevents true parallel execution in threads
- Multiprocessing uses separate processes, bypassing the GIL completely
- Web scraping operations can now run truly in parallel with the FastAPI server

### 2. **Better Resource Isolation**
- Scraper process has its own memory space
- Crashes in the scraper won't affect the main API server
- Easier to monitor and debug with separate process IDs

### 3. **Improved Stability**
- Process crashes are isolated and can be restarted independently
- Memory leaks in scraper won't affect the API server
- Better cleanup on shutdown

## Technical Changes

### Files Modified
1. **fastapi_background.py** - Core changes:
   - Replaced `threading.Thread` with `multiprocessing.Process`
   - Replaced `threading.Lock` with `multiprocessing.Manager().dict()` for shared state
   - Added `_scraper_worker()` function to run in separate process
   - Added `stop_scraper_process()` for graceful shutdown
   - Database instance is recreated in subprocess with configuration dict

2. **fastapi_app.py** - Integration changes:
   - Changed `start_scraper_thread()` to `start_scraper_process()`
   - Added `@app.on_event("shutdown")` handler for cleanup
   - Already had `if __name__ == "__main__"` guard (required on Windows)

### Key Implementation Details

#### Shared State Management
```python
# Before (threading)
scraper_state = "idle"
scraper_lock = threading.Lock()

# After (multiprocessing)
_manager = multiprocessing.Manager()
scraper_state_dict = _manager.dict()
scraper_state_dict['state'] = 'idle'
```

#### Process Lifecycle
```python
# Start
_scraper_process = multiprocessing.Process(
    target=_scraper_worker,
    args=(database_config,),
    daemon=True
)
_scraper_process.start()

# Stop (graceful with fallback to force kill)
_scraper_process.terminate()
_scraper_process.join(timeout=5)
if _scraper_process.is_alive():
    _scraper_process.kill()
```

#### Status Monitoring
Now includes process health information:
```json
{
  "running": true,
  "state": "scraping",
  "last_check": "2026-01-06T10:30:00",
  "process_alive": true,
  "pid": 12345
}
```

## Usage

### Starting the Application
```bash
# Same as before - process starts automatically
python fastapi_app.py
# or
uvicorn fastapi_app:app --host 0.0.0.0 --port 5000 --reload
```

### Checking Scraper Status
```bash
curl http://localhost:5000/api/scraper-status
```

### Stopping Gracefully
The scraper process will automatically stop when:
- FastAPI application shuts down
- SIGTERM/SIGINT received (Ctrl+C)
- Application crashes (process cleanup)

## Platform Compatibility

### Windows
- **REQUIRED**: `if __name__ == "__main__"` guard (already added)
- **REQUIRED**: All multiprocessing code must be importable
- Process spawning uses "spawn" method by default

### Linux/macOS
- Uses "fork" method by default (faster)
- More efficient process creation
- Shared memory works better

## Monitoring and Debugging

### View Process ID
```python
from fastapi_background import _scraper_process
if _scraper_process and _scraper_process.is_alive():
    print(f"Scraper PID: {_scraper_process.pid}")
```

### System-Level Monitoring
```bash
# Linux/macOS
ps aux | grep python

# Windows
tasklist | findstr python
```

### Kill Process Manually (if needed)
```bash
# Linux/macOS
kill -9 <PID>

# Windows
taskkill /F /PID <PID>
```

## Migration Checklist

✅ Replaced threading with multiprocessing
✅ Added Manager for shared state
✅ Recreate database in subprocess
✅ Added graceful shutdown handler
✅ Verified Windows compatibility (`if __name__ == "__main__"`)
✅ Updated status endpoint with PID
✅ Added force kill fallback
✅ Tested process isolation

## Backward Compatibility

- API endpoints unchanged
- Database format unchanged
- Configuration unchanged
- Same functionality, better performance

## Future Enhancements

1. **Process Pool** - For scraping multiple worlds in parallel
2. **IPC Queue** - For real-time communication between processes
3. **Health Monitoring** - Auto-restart dead processes
4. **Resource Limits** - Set CPU/memory limits per process
