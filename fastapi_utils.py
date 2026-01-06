"""
Utility functions for the FastAPI application.
"""
import os
import queue
import time
import threading
from datetime import datetime, timedelta
import httpx
from pebble import ThreadPool
from concurrent.futures import TimeoutError, as_completed


# Configuration
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
FORCE_PROXY = True if os.environ.get('FORCE_PROXY', None) == 'true' else False

# Console log queue for real-time display
console_queue = queue.Queue()
delta_queue = queue.Queue()


def log_console(message, level="INFO"):
    """Log message to console and queue"""
    timestamp = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    console_queue.put(log_entry)


def parse_datetime(date_str):
    """Parse datetime from Brazilian format"""
    import re
    if "Hoje" in date_str:
        time_part = re.search(r'(\d{2}:\d{2})', date_str)
        if time_part:
            time_str = time_part.group(1)
            now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
            date_time = datetime.strptime(f"{now.date()} {time_str}", "%Y-%m-%d %H:%M")
            import pandas as pd
            return pd.to_datetime(date_time)
        else:
            yesterday = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)) - timedelta(days=1)
            import pandas as pd
            return pd.to_datetime(datetime.strptime(f"{yesterday.date()} 00:00", "%Y-%m-%d %H:%M"))
    return None


def parse_online_time_to_minutes(time_str):
    """Parse online time string to total minutes
    Examples: '6h 05m' -> 365, '7h 10m' -> 430, '50m' -> 50
    """
    if not time_str or time_str == "0:00":
        return 0
    
    total_minutes = 0
    time_str = time_str.strip()
    
    # Extract hours if present
    if 'h' in time_str:
        parts = time_str.split('h')
        hours = int(parts[0].strip())
        total_minutes += hours * 60
        # Get remaining part after 'h'
        remaining = parts[1].strip() if len(parts) > 1 else ""
    else:
        remaining = time_str
    
    # Extract minutes if present
    if 'm' in remaining:
        minutes_str = remaining.split('m')[0].strip()
        if minutes_str:
            minutes = int(minutes_str)
            total_minutes += minutes
    
    return total_minutes


def get_multiple(url: str, proxies: list):
    """
    Make multiple concurrent requests through different proxies.
    Returns the first successful response.
    """
    tic = time.time()
    
    # Local success flag - not global
    success_flag = threading.Event()
    
    def get_resp(url, proxy):
        # Check if another thread already succeeded
        if success_flag.is_set():
            print(f"Skipping {proxy} - already got success")
            return None
            
        try:
            with httpx.Client(proxy=proxy) as client:
                tic_req=time.time()
                print(f"Sending request via proxy: {proxy}")
                
                # Check again before making request
                if success_flag.is_set():
                    return None
                    
                response = client.get(url, timeout=30)
                
                # Check if we should even process this
                if success_flag.is_set():
                    return None
                    
                toc_req=time.time()
                print(f"Response time via proxy {proxy}: {toc_req-tic_req:.2f}s")
                print(f"Received response via proxy: {proxy} with status code {response.status_code}")
                
                # Return both status and content so we can check it
                return {
                    "object":response,
                    'status_code': response.status_code,
                    'proxy': proxy,
                    'time': toc_req-tic_req
                }
        except Exception as e:
            if not success_flag.is_set():
                print(f"Error with {proxy}: {str(e)}")
            return None
    
    pool = ThreadPool(max_workers=40)
    
    try:
        # Submit all tasks
        futures = [pool.schedule(get_resp, args=(url, proxy)) for proxy in proxies]
        
        # Use as_completed to get results as they finish (not in order!)
        for future in as_completed(futures):
            # If we already found success, break immediately
            if success_flag.is_set():
                break
                
            try:
                result = future.result(timeout=0.01)
                
                # Check if it's a successful response
                if result and isinstance(result, dict) and result.get('status_code') == 200:
                    toc = time.time()
                    print(f"\n✓ SUCCESS! Total time: {toc-tic:.2f}s")
                    print(f"✓ Successful response via proxy: {result['proxy']}")
                    
                    # Signal all other threads to stop
                    success_flag.set()
                    # Return IMMEDIATELY - don't wait for anything
                    return result['object']
                    
            except TimeoutError:
                pass
            except Exception as e:
                pass
                
    finally:
        # Close pool without waiting
        pool.close()
        
    return None


# Proxy list
PROXY_LIST = [
    'http://103.155.62.141:8081',
    'http://45.177.16.137:999',
    'http://190.242.157.215:8080',
    'http://187.102.219.64:999',
    'http://41.128.72.147:1981',
    'http://62.113.119.14:8080',
    'http://59.6.25.118:3128',
    'http://101.47.16.15:7890',
    'http://154.3.236.202:3128',
    'http://194.26.141.202:3128',
    'http://205.164.192.115:999'
]
