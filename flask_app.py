import os
import sys
import threading
import time
import queue
import json
import gc
from io import StringIO
from flask import Flask, render_template, jsonify, request, Response
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import bs4
import werkzeug.exceptions
import traceback
import httpx
from pebble import ThreadPool
from concurrent.futures import TimeoutError, as_completed
import time
import threading

# Configure aggressive garbage collection for memory efficiency
gc.set_threshold(700, 10, 5)  # More aggressive than default (700, 10, 10)
gc.enable()

app = Flask(__name__)

# Wrap Flask app for ASGI compatibility with uvicorn

# Configuration from environment variables
UPLOAD_PASSWORD = os.environ.get('UPLOAD_PASSWORD', 'Rollabostx1234')
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')
#DATA_FOLDER = os.environ.get('DATA_FOLDER', 'var/data')
DATA_FOLDER = os.environ.get('DATA_FOLDER', '/var/data')
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
DAILY_RESET_HOUR = int(os.environ.get('DAILY_RESET_HOUR', '10'))
DAILY_RESET_MINUTE = int(os.environ.get('DAILY_RESET_MINUTE', '2'))

FORCE_PROXY=True if os.environ.get('FORCE_PROXY', None) == 'true' else False

# Error Handlers
@app.errorhandler(400)
def handle_bad_request(e):
    """Handle 400 Bad Request errors"""
    return jsonify({
        'error': 'Bad Request',
        'message': str(e),
        'status': 400
    }), 400

@app.errorhandler(404)
def handle_not_found(e):
    """Handle 404 Not Found errors"""
    return jsonify({
        'error': 'Not Found',
        'message': str(e),
        'status': 404
    }), 404

@app.errorhandler(405)
def handle_method_not_allowed(e):
    """Handle 405 Method Not Allowed errors"""
    return jsonify({
        'error': 'Method Not Allowed',
        'message': str(e),
        'status': 405
    }), 405

@app.errorhandler(500)
def handle_internal_error(e):
    """Handle 500 Internal Server errors"""
    error_msg = str(e)
    log_console(f"Internal server error: {error_msg}", "ERROR")
    return jsonify({
        'error': 'Internal Server Error',
        'message': error_msg,
        'status': 500
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Handle all unhandled exceptions"""
    error_msg = str(e)
    error_type = type(e).__name__
    
    # Log the full traceback for debugging
    log_console(f"Unhandled exception ({error_type}): {error_msg}", "ERROR")
    
    # Return a clean JSON error response
    return jsonify({
        'error': error_type,
        'message': error_msg,
        'status': 500
    }), 500
pp=['http://103.155.62.141:8081',
 'http://45.177.16.137:999',
 'http://190.242.157.215:8080',
 'http://187.102.219.64:999',
 'http://41.128.72.147:1981',
 'http://62.113.119.14:8080',
 'http://59.6.25.118:3128',
 'http://101.47.16.15:7890',
 'http://154.3.236.202:3128',
 'http://194.26.141.202:3128',
 'http://205.164.192.115:999']



def get_multiple(url: str, proxies: list):
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



# Console log queue for real-time display
console_queue = queue.Queue()
delta_queue = queue.Queue()
scraper_running = False
scraper_state = "idle"  # idle, checking, scraping, sleeping
scraper_lock = threading.Lock()
last_status_check = None



class Database:
    """
    Database abstraction layer for storing player EXP data.
    Currently uses CSV files, designed to be easily swappable with SQLite.
    """
    def __init__(self, folder=None):
        if folder is None:
            folder = DATA_FOLDER
        self.folder = folder
        self.exps_file = f"{folder}/exps.csv"
        self.deltas_file = f"{folder}/deltas.csv"
        self.reset_date_file = f"{folder}/last_reset.txt"
        self.status_data_file = f"{folder}/status_data.json"
        self.scraping_data_file = f"{folder}/scraping_data.json"
        self.vips_file = f"{folder}/vips.txt"
        self.vipsdata_file = f"{folder}/vipsdata.csv"
        self.deltavip_file = f"{folder}/deltavip.csv"
        self.lock = threading.Lock()
        self.reset_done_today = False  # Flag to avoid multiple reset checks
        self.skip_next_deltas = False  # Flag to skip first delta after daily reset
        
        # Ensure data directory exists
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        # Initialize scraping config if it doesn't exist
        self._initialize_scraping_config()
        self._initialize_vip_files()
    
    def _read_exps(self):
        """Read exps table from storage"""
        try:
            df = pd.read_csv(self.exps_file, dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str}, parse_dates=['last update'])
            if 'exp' not in df.select_dtypes(include=['int']).columns:
                df['exp'] = df['exp'].astype('int64')
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated exps: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated exps: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                # Save migrated data
                self._write_exps(df)
            
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'exp', 'last update', 'world', 'guild'])
    
    def _read_deltas(self):
        """Read deltas table from storage"""    
        try:
            df = pd.read_csv(self.deltas_file, dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str}, parse_dates=['update time'])
            if 'deltaexp' not in df.select_dtypes(include=['int']).columns:
                df['deltaexp'] = df['deltaexp'].astype('int64')
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated deltas: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated deltas: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                # Save migrated data
                self._write_deltas(df)
            
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'deltaexp', 'update time', 'world', 'guild'])
    
    def _write_exps(self, df):
        """Write exps table to storage"""
        df.to_csv(self.exps_file, index=False)
    
    def _write_deltas(self, df):
        """Write deltas table to storage"""
        df.to_csv(self.deltas_file, index=False)
    
    def _read_status_data(self):
        """Read status data from JSON file"""
        try:
            with open(self.status_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
    
    def _write_status_data(self, data):
        """Write status data to JSON file"""
        with open(self.status_data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def get_status_data(self):
        """Get status data with lock"""
        with self.lock:
            return self._read_status_data()
    
    def save_status_data(self, data):
        """Save status data with lock"""
        with self.lock:
            self._write_status_data(data)
    
    def _initialize_scraping_config(self):
        """Initialize scraping config with default if not exists"""
        if not os.path.exists(self.scraping_data_file):
            default_config = [
                {
                    "world": DEFAULT_WORLD,
                    "guilds": [DEFAULT_GUILD]
                }
            ]
            with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            log_console(f"Created default scraping config: {default_config}", "INFO")
    
    def get_scraping_config(self):
        """Get scraping configuration"""
        try:
            with open(self.scraping_data_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Validate config structure
                if not isinstance(config, list):
                    raise ValueError("Config must be an array")
                for item in config:
                    if 'world' not in item or 'guilds' not in item:
                        raise ValueError("Each config item must have 'world' and 'guilds' fields")
                    if not isinstance(item['guilds'], list):
                        raise ValueError("'guilds' must be an array")
                return config
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            log_console(f"Error reading scraping config: {str(e)}, using default", "WARNING")
            self._initialize_scraping_config()
            return self.get_scraping_config()
    
    def save_scraping_config(self, config):
        """Save scraping configuration"""
        with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        log_console(f"Scraping config updated: {len(config)} world(s)", "INFO")

    def load(self, folder=None):
        """Initialize database (for compatibility, now reads on-demand)"""
        if folder:
            self.folder = folder
            self.exps_file = f"{folder}/exps.csv"
            self.deltas_file = f"{folder}/deltas.csv"
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        with self.lock:
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            # Check and fix duplicates in deltas
            if not deltas.empty:
                duplicates = deltas[deltas.duplicated(subset=['name', 'update time'], keep=False)]
                if not duplicates.empty:
                    original_count = len(deltas)
                    # Keep last occurrence (most recent in file)
                    deltas = deltas.drop_duplicates(subset=['name', 'update time'], keep='last')
                    removed = original_count - len(deltas)
                    log_console(f"Found and removed {removed} duplicate deltas on load", "WARNING")
                    # Save cleaned data
                    self._write_deltas(deltas)
        
        log_console(f"Database initialized: {len(exps)} players, {len(deltas)} deltas")

    def save(self, folder=None):
        """Save database (for compatibility, now writes happen immediately)"""
        log_console("Database persisted to CSV files")

    def update(self, df, update_time):
        """Update player EXP data and record deltas"""
        with self.lock:
            # Read current data
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            # Calculate previous update time once using distinct update times
            if not deltas.empty:
                distinct_times = deltas['update time'].unique()
                prev_times = [t for t in distinct_times if t < update_time]
                prev_update_time = max(prev_times) if prev_times else update_time
            else:
                prev_update_time = update_time

            # Create index for faster lookups
            exps_dict = exps.set_index('name')[['exp']].to_dict('index')
            deltas_set = set(zip(deltas['name'], deltas['update time']))
            
            # Collect new rows instead of appending one-by-one
            new_deltas = []
            new_exps = []
            exps_updates = {}  # name -> updates dict
            deltas_updates = {}  # (name, time) -> deltaexp

            # Process each player
            for row in df.itertuples(index=False):
                name = row.name
                exp = int(row.exp)
                # Column 'last update' has a space, access with getattr or position
                last_update = getattr(row, 'last_update', getattr(row, '_2', None))
                world = getattr(row, 'world', DEFAULT_WORLD)
                guild = getattr(row, 'guild', DEFAULT_GUILD)

                if name in exps_dict:
                    # Existing player - calculate delta
                    prev_exp = exps_dict[name]['exp']
                    deltaexp = exp - prev_exp
                    if deltaexp != 0 and not self.skip_next_deltas:
                        delta_key = (name, update_time)
                        if delta_key not in deltas_set:
                            new_deltas.append({
                                'name': name, 
                                'deltaexp': deltaexp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            })
                            log_console(f"EXP gain: {name} +{deltaexp} ({world} - {guild})")
                        else:
                            # Duplicate found - mark for update
                            deltas_updates[delta_key] = deltaexp
                            log_console(f"Updated duplicate for {name} at {update_time} (latest)", "INFO")
                        
                        # Broadcast to delta stream
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(deltaexp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat(),
                            'world': world,
                            'guild': guild
                        })
                    elif deltaexp != 0 and self.skip_next_deltas:
                        log_console(f"Skipping first delta after reset for {name}: {deltaexp} ({world} - {guild})", "INFO")
                    
                    # Mark player for update
                    exps_updates[name] = {'exp': exp, 'last update': last_update, 'world': world, 'guild': guild}
                else:
                    # New player
                    new_exps.append({
                        'name': name, 
                        'exp': exp, 
                        'last update': last_update,
                        'world': world,
                        'guild': guild
                    })
                    
                    # Skip delta for new players if we just reset
                    if not self.skip_next_deltas:
                        delta_key = (name, update_time)
                        if delta_key not in deltas_set:
                            new_deltas.append({
                                'name': name, 
                                'deltaexp': exp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            })
                            log_console(f"New player: {name} with {exp} EXP ({world} - {guild})")
                        else:
                            deltas_updates[delta_key] = exp
                            log_console(f"Updated duplicate for new player {name} at {update_time} (latest)", "INFO")
                        
                        # Broadcast to delta stream
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(exp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat(),
                            'world': world,
                            'guild': guild
                        })
                    else:
                        log_console(f"Skipping first delta after reset for new player {name}: {exp} ({world} - {guild})", "INFO")
            
            # Free df after processing all rows
            del df
            
            # Apply updates efficiently
            for name, updates in exps_updates.items():
                mask = exps['name'] == name
                for col, val in updates.items():
                    exps.loc[mask, col] = val
            
            for (name, time), deltaexp in deltas_updates.items():
                mask = (deltas['name'] == name) & (deltas['update time'] == time)
                deltas.loc[mask, 'deltaexp'] = deltaexp
            
            # Add new rows in batch
            if new_exps:
                exps = pd.concat([exps, pd.DataFrame(new_exps)], ignore_index=True)
            if new_deltas:
                deltas = pd.concat([deltas, pd.DataFrame(new_deltas)], ignore_index=True)
            
            # Reset the skip flag after processing all players in this update
            if self.skip_next_deltas:
                self.skip_next_deltas = False
                log_console("Reset skip_next_deltas flag - next update will record deltas normally", "INFO")
            
            # Write changes to storage immediately
            self._write_exps(exps)
            self._write_deltas(deltas)
            
            # Free memory
            del exps_dict, deltas_set, new_deltas, new_exps, exps_updates, deltas_updates, exps, deltas
            gc.collect()
    
    def get_exps(self):
        """Get all player EXP data"""
        with self.lock:
            return self._read_exps()
    
    def get_deltas(self):
        """Get all delta records"""
        with self.lock:
            return self._read_deltas()
    
    def check_and_reset_daily(self, update_time=None):
        """Check if daily reset is needed before first valid update after 10:02 AM"""
        # Quick check: if we already reset today, skip all file I/O
        if self.reset_done_today:
            return False
        
        # If no update_time provided, just check without resetting
        if update_time is None:
            return False
        
        with self.lock:
            now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)  # Apply timezone offset
            today_str = now.strftime("%Y-%m-%d")
            
            # Convert update_time to datetime if it's not already
            if isinstance(update_time, str):
                update_time = pd.to_datetime(update_time)
            
            # Check if update is after daily reset time
            reset_time = now.replace(hour=DAILY_RESET_HOUR, minute=DAILY_RESET_MINUTE, second=0, microsecond=0)
            update_datetime = update_time
            if isinstance(update_datetime, pd.Timestamp):
                update_datetime = update_datetime.to_pydatetime()
            
            # If update is before 10:02, no reset needed
            if update_datetime.time() < reset_time.time():
                return False
            
            # Check last reset date
            try:
                with open(self.reset_date_file, 'r') as f:
                    last_reset = f.read().strip()
                if last_reset == today_str:
                    self.reset_done_today = True  # Mark flag
                    return False  # Already reset today
            except FileNotFoundError:
                pass  # No previous reset file, proceed with reset
            
            # Perform reset before processing this update
            log_console("Daily ranking reset triggered before first update after 10:02 AM", "INFO")
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            if not exps.empty:
                # Create final deltas for today before reset to preserve history
                reset_timestamp = pd.to_datetime(reset_time)
           
                
                # Reset all EXP values to 0
                exps['exp'] = 0
                exps['last update'] = reset_timestamp
                
                # Save changes
                self._write_exps(exps)
                self._write_deltas(deltas)
                
                log_console(f"Reset {len(exps)} players' EXP to 0. Historical data preserved.", "SUCCESS")
            
            # Update last reset date
            with open(self.reset_date_file, 'w') as f:
                f.write(today_str)
            
            self.reset_done_today = True  # Mark flag after successful reset
            self.skip_next_deltas = True  # Skip the first delta after reset
            log_console("Set skip_next_deltas flag - next update will skip recording deltas", "INFO")
            
            # Also reset VIP data
            self._reset_vip_daily()
            
            return True
    
    def _initialize_vip_files(self):
        """Initialize VIP tracking files if they don't exist"""
        # Initialize vips.txt
        if not os.path.exists(self.vips_file):
            with open(self.vips_file, 'w', encoding='utf-8') as f:
                f.write("")
            log_console("Created vips.txt", "INFO")
        
        # Initialize vipsdata.csv
        if not os.path.exists(self.vipsdata_file):
            df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            df.to_csv(self.vipsdata_file, index=False)
            log_console("Created vipsdata.csv", "INFO")
        
        # Initialize deltavip.csv
        if not os.path.exists(self.deltavip_file):
            df = pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
            df.to_csv(self.deltavip_file, index=False)
            log_console("Created deltavip.csv", "INFO")
    
    def get_vips(self):
        """Get list of VIP players"""
        with self.lock:
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        name, world = line.split(',', 1)
                        vips.append({'name': name.strip(), 'world': world.strip()})
                return vips
            except FileNotFoundError:
                return []
    
    def add_vip(self, name, world):
        """Add a VIP player"""
        with self.lock:
            # Read VIPs directly to avoid nested lock
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        vip_name, vip_world = line.split(',', 1)
                        vips.append({'name': vip_name.strip(), 'world': vip_world.strip()})
            except FileNotFoundError:
                vips = []
            
            # Check if already exists
            for vip in vips:
                if vip['name'] == name and vip['world'] == world:
                    return False
            # Add new VIP
            with open(self.vips_file, 'a', encoding='utf-8') as f:
                f.write(f"{name},{world}\n")
            log_console(f"Added VIP: {name} ({world})", "SUCCESS")
            return True
    
    def remove_vip(self, name, world):
        """Remove a VIP player"""
        with self.lock:
            # Read VIPs directly to avoid nested lock
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        vip_name, vip_world = line.split(',', 1)
                        vips.append({'name': vip_name.strip(), 'world': vip_world.strip()})
            except FileNotFoundError:
                vips = []
            
            new_vips = [v for v in vips if not (v['name'] == name and v['world'] == world)]
            if len(new_vips) == len(vips):
                return False
            # Write back
            with open(self.vips_file, 'w', encoding='utf-8') as f:
                for vip in new_vips:
                    f.write(f"{vip['name']},{vip['world']}\n")
            log_console(f"Removed VIP: {name} ({world})", "SUCCESS")
            return True
    
    def get_vipsdata(self):
        """Get current VIP data"""
        with self.lock:
            try:
                df = pd.read_csv(self.vipsdata_file, dtype={'name': str, 'world': str, 'today_exp': 'int64', 'today_online': 'int64'})
                return df
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
    
    def update_vipdata(self, name, world, today_exp, today_online):
        """Update VIP today's data"""
        with self.lock:
            # Read VIP data directly to avoid nested lock
            try:
                df = pd.read_csv(self.vipsdata_file, dtype={'name': str, 'world': str, 'today_exp': 'int64', 'today_online': 'int64'})
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            
            # Check if entry exists
            mask = (df['name'] == name) & (df['world'] == world)
            if mask.any():
                df.loc[mask, 'today_exp'] = today_exp
                df.loc[mask, 'today_online'] = today_online
            else:
                new_row = pd.DataFrame([{
                    'name': name,
                    'world': world,
                    'today_exp': today_exp,
                    'today_online': today_online
                }])
                df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(self.vipsdata_file, index=False)
    
    def get_deltavip(self):
        """Get VIP delta history"""
        with self.lock:
            try:
                df = pd.read_csv(self.deltavip_file, 
                                dtype={'name': str, 'world': str, 'date': str, 'delta_exp': 'int64', 'delta_online': 'int64'},
                                parse_dates=['update_time'])
                return df
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
    
    def add_vip_delta(self, name, world, date, delta_exp, delta_online, update_time):
        """Add VIP delta record"""
        with self.lock:
            # Read VIP delta data directly to avoid nested lock
            try:
                df = pd.read_csv(self.deltavip_file,
                                dtype={'name': str, 'world': str, 'date': str, 'delta_exp': 'int64', 'delta_online': 'int64'},
                                parse_dates=['update_time'])
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
            
            new_row = pd.DataFrame([{
                'name': name,
                'world': world,
                'date': date,
                'delta_exp': delta_exp,
                'delta_online': delta_online,
                'update_time': update_time
            }])
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(self.deltavip_file, index=False)
            log_console(f"VIP delta: {name} ({world}) +{delta_exp} exp, +{delta_online} online", "INFO")
    
    def update_last_vip_delta_time(self, name, world, new_update_time):
        """Update the update_time of the last delta entry for a VIP"""
        with self.lock:
            try:
                df = pd.read_csv(self.deltavip_file,
                                dtype={'name': str, 'world': str, 'date': str, 'delta_exp': 'int64', 'delta_online': 'int64'},
                                parse_dates=['update_time'])
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return False
            
            # Filter for this VIP's entries
            vip_mask = (df['name'] == name) & (df['world'] == world)
            if not vip_mask.any():
                return False
            
            # Get the last entry index
            vip_entries = df[vip_mask]
            last_idx = vip_entries.index[-1]
            
            # Update the timestamp
            df.at[last_idx, 'update_time'] = new_update_time
            df.to_csv(self.deltavip_file, index=False)
            log_console(f"Updated VIP delta timestamp: {name} ({world}) to {new_update_time}", "INFO")
            return True
    
    def _reset_vip_daily(self):
        """Reset VIP data at daily reset"""
        with self.lock:
            # Clear today's data
            df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            df.to_csv(self.vipsdata_file, index=False)
            log_console("Reset VIP daily data", "INFO")


# Logging function
def log_console(message, level="INFO"):
    timestamp = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    console_queue.put(log_entry)



















# Scraper functions from notebook
def extract_tables(soup):
    """Extract all tables from BeautifulSoup object"""
    dataframes = []
    table_elements = soup.find_all('table')

    for i, table in enumerate(table_elements):
        headers = []
        rows = []

        header_elements = table.find_all('th')
        if header_elements:
            headers = [header.get_text(strip=True) for header in header_elements]
        else:
            first_row = table.find('tr')
            if first_row:
                first_row_cells = first_row.find_all(['td', 'th'])
                headers = [cell.get_text(strip=True) for cell in first_row_cells]

        row_elements = table.find_all('tr')
        for row in row_elements:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                if row_data != headers:
                    rows.append(row_data)

        if rows:
            if headers and len(headers) == len(rows[0]):
                df = pd.DataFrame(rows, columns=headers)
            else:
                df = pd.DataFrame(rows)
            df.attrs['table_index'] = i
            dataframes.append(df)

    del rows, headers
    gc.collect()
    return dataframes


def scrape_player_data(name):
    """Complete pipeline to scrape player data from rubinothings.com.br"""
    result = {
        'name': name,
        'tables': [],
        'response_status': None,
        'success': False
    }
    
    url = "https://rubinothings.com.br/player"
    params = {"name": name}
    
    # Try direct fetch first
    if not FORCE_PROXY:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed for '{name}' with status {response.status_code}, trying proxies...", "WARNING")
                # Build URL with params for proxy attempt
                url_with_params = f"{url}?name={name.replace(' ', '+')}"
                response = get_multiple(url_with_params, pp)
                log_console(f"Proxy fetch response for '{name}': {response}", "INFO")
        except Exception as e:
            log_console(f"Direct fetch failed for '{name}': {str(e)}, trying proxies...", "WARNING")
            url_with_params = f"{url}?name={name.replace(' ', '+')}"
            response = get_multiple(url_with_params, pp)
            log_console(f"Proxy fetch response for '{name}': {response}", "INFO")
    else:
        url_with_params = f"{url}?name={name.replace(' ', '+')}"
        response = get_multiple(url_with_params, pp)
        log_console(f"Proxy fetch response for '{name}': {response}", "INFO")

    if not response:
        log_console(f"All requests failed for '{name}'", "ERROR")
        return result
    
    try:
        result['response_status'] = response.status_code if hasattr(response, 'status_code') else 200
        
        if result['response_status'] == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = extract_tables(soup)
            
            # Convert DataFrames to dict format for JSON serialization
            tables_dict = []
            for df in tables:
                tables_dict.append({
                    'columns': df.columns.tolist(),
                    'data': df.values.tolist()
                })
            
            result['tables'] = tables_dict
            result['success'] = True
            log_console(f"Successfully scraped data for '{name}' - Found {len(tables)} tables", "INFO")
            
            # Free memory
            del soup, tables, tables_dict
        else:
            log_console(f"Request failed for '{name}' with status code: {result['response_status']}", "ERROR")
            
    except Exception as e:
        log_console(f"Error parsing player data for '{name}': {str(e)}", "ERROR")
    
    del response
    gc.collect()
    return result


def parse_datetime(date_str):
    """Parse datetime from Brazilian format"""
    import re
    if "Hoje" in date_str:
        time_part = re.search(r'(\d{2}:\d{2})', date_str)
        if time_part:
            time_str = time_part.group(1)
            now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
            date_time = datetime.strptime(f"{now.date()} {time_str}", "%Y-%m-%d %H:%M")
            return pd.to_datetime(date_time)
        else:
            yesterday = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)) - timedelta(days=1)
            return pd.to_datetime(datetime.strptime(f"{yesterday.date()} 00:00", "%Y-%m-%d %H:%M"))
    return None


def get_ranking(world=None, guildname=None):
    """Get ranking from website"""
    if world is None:
        world = DEFAULT_WORLD
    if guildname is None:
        guildname = DEFAULT_GUILD
    url = f"https://rubinothings.com.br/guild.php?guild={guildname.replace(' ', '+')}&world={world}"

    # Try direct fetch first
    if not FORCE_PROXY:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, pp)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    else:
        response = get_multiple(url, pp)
    
    if not response:
        return None
    print(response)

    soup = BeautifulSoup(response.text, 'html.parser')
    return extract_tables(soup)


def get_last_status_updates(world=None):
    """Get status updates to determine correct scraping timestamp"""
    if world is None:
        world = DEFAULT_WORLD
    url = "https://rubinothings.com.br/status"
    
    # Try direct fetch first

    if not FORCE_PROXY:

        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, pp)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    else:
        response = get_multiple(url, pp)
    
    if not response:
        return None
    soup = bs4.BeautifulSoup(response.text, 'html.parser')

    r = extract_tables(soup)
    split_tables = []

    for df in r:
        mask = df.apply(lambda row: all((str(x).strip() == '' or pd.isna(x)) for x in row), axis=1)
        split_indices = mask[mask].index.tolist()
        prev = 0
        for idx in split_indices:
            part = df.iloc[prev:idx]
            if not part.empty:
                split_tables.append(part.reset_index(drop=True))
            prev = idx + 1
        part = df.iloc[prev:]
        if not part.empty:
            split_tables.append(part.reset_index(drop=True))

    tables_dict = {}
    for table in split_tables:
        if not table.empty:
            table_name = str(table.iloc[0, 0]).strip()
            if "Status" in table_name:
                table_name = table_name.split("Status")[0].strip()
            new_table = table.iloc[1:].reset_index(drop=True)
            tables_dict[table_name] = new_table

    for key in tables_dict:
        df = tables_dict[key]
        if not df.empty:
            df.iloc[:, 0] = (
                df.iloc[:, 0]
                .str.replace("Rotina de coleta", "")
                .str.replace(r"[^\w\s,.:-]", "", regex=True)
            )
            tables_dict[key] = df
            if df.shape[1] >= 4:
                df.columns = ["rotina", "last update", "time_outdated", "status"]
                tables_dict[key] = df

    for key in tables_dict:
        df = tables_dict[key]
        if 'last update' in df.columns:
            df['last update'] = df['last update'].apply(parse_datetime)
            tables_dict[key] = df

    # Clean up
    del soup, r, split_tables, response
    gc.collect()
    return tables_dict


def return_last_update(world=None, save_all_data=True, database=None):
    """Get the last update time and optionally save all worlds data to JSON"""
    if world is None:
        world = DEFAULT_WORLD
    if save_all_data and database:
        # Fetch all status data
        all_status = get_last_status_updates(world)
        
        if all_status:
            # Convert to JSON-serializable format
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    # Convert DataFrame to dict and handle datetime
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            # Apply timezone offset before converting to ISO format
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            # Save to JSON file
            database.save_status_data(json_data)
            log_console(f"Status data saved for {len(json_data['worlds'])} worlds", "INFO")
    
    # Get the specific world's update time
    status_data = get_last_status_updates(world)
    if status_data and world in status_data:
        df = status_data[world]
        update_time = str(df[df['rotina'] == 'Daily Raw Ranking']['last update'].values[0])
        return pd.to_datetime(update_time)
    return None


def parse_to_db_formatted(df, last_update, world=None, guild=None):
    """Parse ranking data to database format"""
    if world is None:
        world = DEFAULT_WORLD
    if guild is None:
        guild = DEFAULT_GUILD
    new_df = pd.DataFrame()
    new_df['name'] = df['Jogador']
    new_df['exp'] = df['RAW no período'].str.replace(',', '').str.replace('.', '').astype(int)
    new_df['last update'] = last_update
    new_df['world'] = world
    new_df['guild'] = guild
    return new_df


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


def scrape_single_vip(database, name, world):
    """Scrape a single VIP player and update their data"""
    try:
        result = scrape_player_data(name)
        if result['success'] and result['tables']:
            # Find the Raw XP table (table with "Raw XP no dia" column)
            today_exp = 0
            today_online = 0  # minutes
            
            for table in result['tables']:
                columns = table['columns']
                data = table['data']
                
                # Check for Raw XP table
                if 'Raw XP no dia' in columns and data:
                    idx = columns.index('Raw XP no dia')
                    # Get the first row's value
                    raw_value = data[0][idx] if len(data[0]) > idx else "0"
                    # Remove formatting and convert
                    today_exp = int(raw_value.replace(',', '').replace('.', ''))
                
                # Check for Online time table
                if 'Online time' in columns and data:
                    idx = columns.index('Online time')
                    online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                    # Parse to minutes
                    today_online = parse_online_time_to_minutes(online_time_str)
            
            # Get OLD values from vipsdata BEFORE updating
            vipsdata = database.get_vipsdata()
            existing_vip = vipsdata[(vipsdata['name'] == name) & (vipsdata['world'] == world)]
            
            if not existing_vip.empty:
                # VIP exists - calculate delta from previous cumulative
                old_exp = existing_vip['today_exp'].values[0]
                old_online = existing_vip['today_online'].values[0]
                delta_exp = today_exp - old_exp
                delta_online = today_online - old_online
                
                now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                today_date = now.strftime("%Y-%m-%d")
                
                # Only process if exp has changed
                if delta_exp != 0:
                    # Save delta (including online time change)
                    database.add_vip_delta(name, world, today_date, delta_exp, delta_online, now)
                    
                    # Update VIP data with NEW values
                    database.update_vipdata(name, world, today_exp, today_online)
                    log_console(f"VIP {name} ({world}): {today_exp} exp, {today_online} min online", "INFO")
                else:
                    # Exp hasn't changed - check last delta
                    deltavip = database.get_deltavip()
                    vip_deltas = deltavip[(deltavip['name'] == name) & (deltavip['world'] == world)]
                    
                    if not vip_deltas.empty:
                        last_delta_exp = vip_deltas.iloc[-1]['delta_exp']
                        
                        if last_delta_exp == 0:
                            # Last delta was also zero - just update timestamp
                            database.update_last_vip_delta_time(name, world, now)
                            log_console(f"VIP {name} ({world}): No exp change, updated last zero timestamp", "INFO")
                        else:
                            # Last delta was non-zero - save new zero delta
                            database.add_vip_delta(name, world, today_date, 0, delta_online, now)
                            log_console(f"VIP {name} ({world}): Saved zero delta (last was non-zero)", "INFO")
                    else:
                        # No previous deltas - save zero
                        database.add_vip_delta(name, world, today_date, 0, delta_online, now)
                        log_console(f"VIP {name} ({world}): First delta (zero)", "INFO")
            else:
                # First time tracking - create initial baseline with 0 delta
                now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                today_date = now.strftime("%Y-%m-%d")
                database.add_vip_delta(name, world, today_date, 0, 0, now)
                database.update_vipdata(name, world, today_exp, today_online)
                log_console(f"VIP delta: {name} ({world}) +0 exp, +0 online (initial baseline)", "INFO")
                log_console(f"VIP {name} ({world}): {today_exp} exp, {today_online} min online", "INFO")
            
            return True
        else:
            log_console(f"Failed to scrape VIP {name} ({world})", "WARNING")
            return False
    except Exception as e:
        log_console(f"Error scraping VIP {name} ({world}): {str(e)}", "ERROR")
        return False


def scrape_vip_data(database, world):
    """Scrape VIP player data for a specific world and update today's stats"""
    vips = database.get_vips()
    if not vips:
        return
    
    # Filter VIPs for this world
    world_vips = [v for v in vips if v['world'] == world]
    if not world_vips:
        return
    
    log_console(f"Scraping {len(world_vips)} VIP players for {world}...", "INFO")
    for vip in world_vips:
        scrape_single_vip(database, vip['name'], vip['world'])


def process_vip_deltas(database, world, update_time):
    """Process VIP data after world update - scraping handles delta calculation"""
    # Simply trigger a scrape for this world's VIPs
    scrape_vip_data(database, world)


def loop_get_rankings(database, debug=False):
    """Background loop to continuously fetch rankings from all configured worlds and guilds"""
    database.load()
    global scraper_running, scraper_state
    scraper_running = True
    
    # Track last update per world (worlds update independently)
    last_updates = {}
    
    # Get scraping configuration
    scraping_config = database.get_scraping_config()
    log_console(f"Starting ranking scraper for {len(scraping_config)} world(s)")
    
    ignore_updates=[]
    while scraper_running:
        try:
            with scraper_lock:
                scraper_state = "checking"
            
            # Get status data for all worlds to check what's available
            all_status = get_last_status_updates()
            
            if not all_status:
                log_console("Failed to get status data, retrying...", "WARNING")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
                continue
            
            # Save all status data to JSON
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            # Apply timezone offset before converting to ISO format
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            database.save_status_data(json_data)
            
            # Check each configured world for updates
            worlds_to_scrape = []
            for config_item in scraping_config:
                world = config_item['world']
                
                # Check if world exists in status data
                if world not in all_status:
                    log_console(f"World '{world}' not found in status data, skipping", "WARNING")
                    continue
                
                # Get update time for this specific world
                df = all_status[world]
                if 'rotina' not in df.columns or 'last update' not in df.columns:
                    log_console(f"Invalid data structure for world '{world}', skipping", "WARNING")
                    continue
                
                daily_raw = df[df['rotina'] == 'Daily Raw Ranking']
                if daily_raw.empty:
                    log_console(f"No 'Daily Raw Ranking' data for world '{world}', skipping", "WARNING")
                    continue
                
                current_update = pd.to_datetime(daily_raw['last update'].values[0])
                
                # Check if this world has a new update
                if world not in last_updates or last_updates[world] != current_update:
                    if current_update not in ignore_updates:
                        worlds_to_scrape.append({
                            'config': config_item,
                            'update_time': current_update
                        })
                        log_console(f"New update for {world}: {last_updates.get(world, 'na')} -> {current_update}", "INFO")
            
            if not worlds_to_scrape:
                if debug:
                    log_console("No new updates found for any world, sleeping 60s", "DEBUG")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
            else:
                # Process EACH WORLD separately with its own timestamp
                with scraper_lock:
                    scraper_state = "scraping"
                
                worlds_updated = 0
                for item in worlds_to_scrape:
                    config_item = item['config']
                    update_time = item['update_time']
                    world = config_item['world']
                    guilds = config_item['guilds']
                    
                    log_console(f"Processing world: {world} at {update_time}", "INFO")
                    
                    # Check for daily reset before processing this world
                    database.check_and_reset_daily(update_time)
                    
                    # Collect all players from all guilds in THIS world
                    world_players = []
                    for guild in guilds:
                        try:
                            log_console(f"Scraping {world} - {guild}", "INFO")
                            r = get_ranking(world=world, guildname=guild)
                            
                            if r is None or len(r) < 2:
                                log_console(f"No data for {world} - {guild}", "WARNING")
                            else:
                                rankings = r[1]
                                rankparsed = parse_to_db_formatted(rankings, update_time, world=world, guild=guild)
                                world_players.append(rankparsed)
                                log_console(f"Got {len(rankparsed)} players from {world} - {guild}", "SUCCESS")
                        except Exception as e:
                            log_console(f"Error scraping {world} - {guild}: {str(e)}", "ERROR")
                    
                    # Update database for THIS WORLD ONLY with ITS timestamp
                    if world_players:
                        combined_df = pd.concat(world_players, ignore_index=True)
                        # Remove duplicates (same player in multiple guilds - keep first)
                        combined_df = combined_df.drop_duplicates(subset=['name'], keep='first')
                        
                        # Update database with THIS world's specific timestamp
                        database.update(combined_df, update_time)
                        database.save()
                        log_console(f"Updated {len(combined_df)} players for {world} at {update_time}", "SUCCESS")
                        
                        # Free memory after database update
                        del combined_df
                        gc.collect()
                        
                        # Scrape VIP data for THIS specific world
                        scrape_vip_data(database, world)
                        
                        # Mark this world as updated and add to ignore list
                        last_updates[world] = update_time
                        if update_time not in ignore_updates:
                            ignore_updates.append(update_time)
                        
                        worlds_updated += 1
                    else:
                        log_console(f"No player data collected for {world}", "WARNING")
                
                if worlds_updated > 0:
                    log_console(f"Successfully updated {worlds_updated} world(s)", "SUCCESS")
                else:
                    log_console("No worlds were updated", "WARNING")
                
                with scraper_lock:
                    scraper_state = "idle"
        except Exception as e:
            log_console(f"Error in scraper: {str(e)}", "ERROR")
            traceback.print_exc()
            with scraper_lock:
                scraper_state = "sleeping"
            time.sleep(10)  # Wait before retry


def start_scraper_thread(database):
    """Start the scraper in a background thread with auto-restart"""
    def run_with_restart():
        while True:
            try:
                log_console("Starting scraper thread...")
                loop_get_rankings(database, debug=True)
            except Exception as e:
                log_console(f"Scraper crashed: {str(e)}. Restarting in 1s...", "ERROR")
                time.sleep(1)

    thread = threading.Thread(target=run_with_restart, daemon=True)
    thread.start()
    log_console("Scraper thread started with auto-restart enabled")

# Initialize database
db = Database()
db.load()

# Start background scraper
start_scraper_thread(db)


def get_delta_between(datetime1, datetime2, database):
    """Filter deltas between two datetimes"""
    table = database.get_deltas()
    datetime1 = pd.to_datetime(datetime1)
    datetime2 = pd.to_datetime(datetime2)
    mask = (table['update time'] >= datetime1) & (table['update time'] <= datetime2)
    return table[mask]


def preprocess_vis_data(all_update_times, all_player_data, names_list):
    """
    Preprocess visualization data to compress consecutive zero periods.
    Returns compressed times and player data with zeros grouped.
    """
    num_times = len(all_update_times)
    
    # Sort by datetime - create sorted indices to maintain data correspondence
    sorted_indices = sorted(range(num_times), key=lambda i: pd.to_datetime(all_update_times[i]))
    
    # Reorder all_update_times and all_player_data according to sorted indices
    all_update_times = [all_update_times[i] for i in sorted_indices]
    all_player_data = {name: [all_player_data[name][i] for i in sorted_indices] for name in names_list}
    

    log_console(all_player_data, "DEBUG")
    # Identify positions where ALL players have zero
    all_zero_positions = []
    for i in range(num_times):
        if all(all_player_data[name][i] == 0 for name in names_list):
            all_zero_positions.append(i)
    
    # Group consecutive zero positions (only if there are 2+ consecutive zeros)
    zero_groups = []
    if all_zero_positions:
        start = all_zero_positions[0]
        for i in range(1, len(all_zero_positions)):
            if all_zero_positions[i] != all_zero_positions[i-1] + 1:
                # End of a consecutive group
                if all_zero_positions[i-1] - start >= 1:  # At least 2 consecutive zeros
                    zero_groups.append((start, all_zero_positions[i-1]))
                start = all_zero_positions[i]
        # Don't forget the last group
        if all_zero_positions[-1] - start >= 1:
            zero_groups.append((start, all_zero_positions[-1]))
    
    # Build compressed timeline - generate labels with metadata for duplicate detection
    compressed_times = []
    compressed_data = {name: [] for name in names_list}
    label_metadata = []  # Store (label, full_label, index) for duplicate detection
    
    prev_time = None
    i = 0
    while i < num_times:
        # Check if this position starts a zero group
        in_zero_group = False
        for start, end in zero_groups:
            if i == start:
                # Create label for zero period
                # Use timestamp BEFORE the zero group starts (if it exists)
                if start > 0:
                    start_time = pd.to_datetime(all_update_times[start - 1])
                else:
                    start_time = pd.to_datetime(all_update_times[start])
                end_time = pd.to_datetime(all_update_times[end])
                
                start_date = start_time.date()
                end_date = end_time.date()
                
                if start_date == end_date:
                    # Same day - short label without date
                    short_label = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
                    full_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%H:%M')}"
                else:
                    # Different days - always show both full dates
                    short_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%d/%m/%Y %H:%M')}"
                    full_label = short_label
                
                label_metadata.append((short_label, full_label, len(compressed_times)))
                compressed_times.append(short_label)
                prev_time = end_time
                
                # Add single zero for each player for this period
                for name in names_list:
                    compressed_data[name].append(0)
                
                i = end + 1
                in_zero_group = True
                break
        
        if not in_zero_group:
            # Regular data point
            time_obj = pd.to_datetime(all_update_times[i])
            current_date = time_obj.date()
            
            # Determine start time for this bucket (previous timestamp or first point)
            if prev_time is None:
                # First data point - show as single time point
                short_label = time_obj.strftime('%H:%M')
                full_label = time_obj.strftime('%d/%m/%Y %H:%M')
            else:
                # Show range from previous timestamp to current
                start_date = prev_time.date()
                
                if start_date == current_date:
                    # Same day - short label without date
                    short_label = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                    full_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                else:
                    # Different days - always show both full dates
                    short_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                    full_label = short_label
            
            label_metadata.append((short_label, full_label, len(compressed_times)))
            compressed_times.append(short_label)
            prev_time = time_obj
            
            for name in names_list:
                compressed_data[name].append(all_player_data[name][i])
            i += 1
    
    # Check for duplicates and replace with full labels where needed
    from collections import Counter
    label_counts = Counter(compressed_times)
    for short_label, full_label, idx in label_metadata:
        if label_counts[short_label] > 1:
            compressed_times[idx] = full_label

    del all_zero_positions, zero_groups, label_metadata, label_counts
    gc.collect()
    return compressed_times, compressed_data


def create_interactive_graph(names, database, datetime1=None, datetime2=None):
    """Create interactive Plotly graph for player EXP gains"""
    # Custom color palette based on theme colors
    theme_colors = [
        '#C21500',  # Primary red-orange
        '#FFC500',  # Primary golden yellow
        '#FF6B35',  # Complementary orange
        '#FFE156',  # Light yellow
        '#B81400',  # Darker red
        '#E6A900',  # Darker yellow
        '#FF8F66',  # Light orange
        '#FFD966',  # Pale yellow
    ]
    
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Handle single name or list of names
    names_list = [names] if isinstance(names, str) else names

    # Get all unique update times across all data (standardized timeline)
    all_update_times = sorted(table['update time'].unique())

    # Build data for all players first
    all_player_data = {}
    for name in names_list:
        player_data = table[table['name'] == name]
        if not player_data.empty:
            player_deltas = dict(zip(player_data['update time'], player_data['deltaexp']))
            standardized_exps = [player_deltas.get(update_time, 0) for update_time in all_update_times]
            all_player_data[name] = standardized_exps

    if not all_player_data:
        fig = go.Figure()
        fig.update_layout(title='No data available')
        return fig.to_json()

    # Preprocess data to compress zero periods
    compressed_times, compressed_data = preprocess_vis_data(all_update_times, all_player_data, names_list)

    print(compressed_times)
    print(compressed_data)
    # Create plotly figure
    fig = go.Figure()

    # Build traces with compressed data
    for idx, name in enumerate(names_list):
        base_color = theme_colors[idx % len(theme_colors)]
        
        # Convert hex to RGB for opacity support
        hex_color = base_color.lstrip('#')
        r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        
        # Add bar trace with gradient colors
        fig.add_trace(go.Bar(
            x=compressed_times,
            y=compressed_data[name],
            name=name,
            marker=dict(
                color=compressed_data[name],  # Use values for gradient
                colorscale=[
                    [0, f'rgba({r},{g},{b},0.2)'],  # Light (20% opacity)
                    [0.5, f'rgba({r},{g},{b},0.6)'],  # Medium (60% opacity)
                    [1, f'rgba({r},{g},{b},1)']  # Full color
                ],
                showscale=False,
                line=dict(width=0)
            ),
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_data[name]],
            textposition='outside',
            textangle=0,
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>'
        ))
        
        # Add smooth spline line on top in light blue
        fig.add_trace(go.Scatter(
            x=compressed_times,
            y=compressed_data[name],
            name=f'{name} (trend)',
            mode='lines',
            line=dict(color='#3498db', width=3, shape='spline'),
            showlegend=False,
            hoverinfo='skip'
        ))
        
      
    fig.update_layout(
        title=f'EXP Gain Over Time',
        xaxis_title='Update Time',
        yaxis_title='Delta EXP',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        barmode='group',  # Group bars side by side for multiple players
        bargap=0,  # No gap between bars
        bargroupgap=0,  # No gap between bar groups
        xaxis=dict(
            type='category',  # Treat x-axis as categorical (string bins)
            categoryorder='array',  # Use explicit array ordering
            categoryarray=compressed_times,  # Exact order from data
            tickangle=-45,
            tickmode='auto',
            nticks=20
        ),
        colorway=theme_colors  # Set default color palette
    )
    fig.update_xaxes(
    categoryorder='array',
    categoryarray=compressed_times
    )

    result = fig.to_json()
    del table, all_update_times, all_player_data, compressed_times, compressed_data, fig
    gc.collect()
    return result


def get_player_stats(names, database, datetime1=None, datetime2=None):
    """Get statistics table for players"""
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Filter by names if specified
    if names:
        names_list = [names] if isinstance(names, str) else names
        table = table[table['name'].isin(names_list)]

    # Group and calculate stats
    stats = table.groupby('name').agg({
        'deltaexp': ['sum', 'mean', 'count', 'max', 'min']
    }).round(2)

    stats.columns = ['Total EXP', 'Average EXP', 'Updates', 'Max EXP', 'Min EXP']
    stats = stats.sort_values('Total EXP', ascending=False)
    stats = stats.reset_index()

    result = stats.to_dict('records')
    del table, stats
    gc.collect()
    return result


@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')


@app.route('/api/players')
def get_players():
    """Get list of all players"""
    deltas = db.get_deltas()
    world = request.args.get('world')
    guild = request.args.get('guild')
    
    # Filter by world and guild if specified
    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]
    
    players = sorted(deltas['name'].unique().tolist())
    return jsonify(players)


@app.route('/api/date-range')
def get_date_range():
    """Get available date range"""
    deltas = db.get_deltas()
    world = request.args.get('world')
    guild = request.args.get('guild')
    
    # Filter by world and guild if specified
    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]
    
    if not deltas.empty:
        min_date = deltas['update time'].min()
        max_date = deltas['update time'].max()
        return jsonify({
            'min': min_date.isoformat(),
            'max': max_date.isoformat()
        })
    return jsonify({'min': None, 'max': None})


@app.route('/api/graph', methods=['POST'])
def get_graph():
    """Generate interactive graph with stats and comparison data"""
    data = request.json
    names = data.get('names', [])
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    if not names:
        return jsonify({'error': 'No players selected'}), 400

    try:
        # Generate individual graph for each player (for carousel)
        # But send the same combined graph data to maintain compatibility
        graph_json = create_interactive_graph(names, db, datetime1, datetime2)

        # Get stats for selected players
        stats = get_player_stats(names, db, datetime1, datetime2)

        # Get comparison data (all players in same time period)
        deltas_table = db.get_deltas()
        exps_table = db.get_exps()

        if datetime1 and datetime2:
            deltas_table = get_delta_between(datetime1, datetime2, db)

        # Calculate overall rankings
        all_rankings = deltas_table.groupby('name')['deltaexp'].sum().sort_values(ascending=False)

        # Get rank for each selected player
        comparison = []
        for name in names:
            if name in all_rankings.index:
                rank = list(all_rankings.index).index(name) + 1
                total_exp = int(all_rankings[name])
                percentile = (1 - (rank / len(all_rankings))) * 100

                # Get current exp from exps table
                current_exp = 0
                if name in exps_table['name'].values:
                    current_exp = int(exps_table[exps_table['name'] == name]['exp'].values[0])

                comparison.append({
                    'name': name,
                    'rank': rank,
                    'total_players': len(all_rankings),
                    'percentile': round(percentile, 1),
                    'total_exp_period': total_exp,
                    'current_total_exp': current_exp
                })

        return jsonify({
            'graph': graph_json,
            'stats': stats,
            'comparison': comparison
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats', methods=['POST'])
def get_stats():
    """Get player statistics (deprecated - use /api/graph instead)"""
    data = request.json
    names = data.get('names', [])
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    try:
        stats = get_player_stats(names, db, datetime1, datetime2)
        return jsonify({'stats': stats})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/top-players', methods=['GET'])
def get_top_players():
    """Get top players by total EXP"""
    limit = request.args.get('limit', 10, type=int)
    datetime1 = request.args.get('datetime1')
    datetime2 = request.args.get('datetime2')

    table = db.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, db)

    # Calculate top players
    top = table.groupby('name')['deltaexp'].sum().sort_values(ascending=False).head(limit)

    result = [{'name': name, 'total_exp': int(exp)} for name, exp in top.items()]
    return jsonify(result)


@app.route('/api/recent-updates', methods=['GET'])
def get_recent_updates():
    """Get recent EXP updates"""
    limit = request.args.get('limit', 20, type=int)

    deltas = db.get_deltas()
    recent = deltas.sort_values('update time', ascending=False).head(limit)
    result = recent.to_dict('records')

    # Convert datetime to string
    for item in result:
        item['update time'] = item['update time'].isoformat()

    return jsonify(result)


@app.route('/api/rankings-table', methods=['POST'])
def get_rankings_table():
    """Get grouped rankings table with filters and sorting"""
    data = request.json
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    try:
        table = db.get_deltas()

        # If dates provided, filter; otherwise show all-time data
        if datetime1 and datetime2:
            table = get_delta_between(datetime1, datetime2, db)

        # Group by name and aggregate
        grouped = table.groupby('name').agg({
            'deltaexp': list,
            'update time': list
        })

        # Use efficient aggregation instead of apply
        grouped['sum'] = grouped['deltaexp'].apply(sum)
        grouped['number of updates'] = grouped['deltaexp'].str.len()
        grouped['avg'] = grouped['sum'] / grouped['number of updates']
        grouped['max'] = grouped['deltaexp'].apply(max)
        grouped['min'] = grouped['deltaexp'].apply(min)

        # Convert to records using to_dict for better performance
        result = [
            {
                'name': name,
                'total_exp': int(row['sum']),
                'updates': int(row['number of updates']),
                'avg_exp': round(row['avg'], 2),
                'max_exp': int(row['max']),
                'min_exp': int(row['min'])
            }
            for name, row in grouped.iterrows()
        ]

        response = jsonify({'rankings': result})
        del table, grouped, result
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error in rankings table: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/console-stream')
def console_stream():
    """Server-Sent Events stream for console logs"""
    def generate():
        # Send initial connection message
        yield f"data: [CONNECTED] Console stream started\n\n"

        while True:
            try:
                # Get log from queue with timeout
                log = console_queue.get(timeout=1)
                yield f"data: {log}\n\n"
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"

    return Response(generate(), mimetype='text/event-stream')


@app.route('/api/scraper-status')
def get_scraper_status():
    """Get scraper status"""
    global last_status_check
    last_status_check = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
    
    deltas = db.get_deltas()
    with scraper_lock:
        state = scraper_state
    
    return jsonify({
        'running': scraper_running,
        'state': state,  # idle, checking, scraping, sleeping
        'last_update': deltas['update time'].max().isoformat() if not deltas.empty else None,
        'last_check': last_status_check.isoformat()
    })


@app.route('/api/download/deltas')
def download_deltas():
    """Download deltas.csv file"""
    from flask import send_file
    try:
        return send_file(db.deltas_file, as_attachment=True, download_name='deltas.csv')
    except Exception as e:
        log_console(f"Error downloading deltas.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/exps')
def download_exps():
    """Download exps.csv file"""
    from flask import send_file
    try:
        return send_file(db.exps_file, as_attachment=True, download_name='exps.csv')
    except Exception as e:
        log_console(f"Error downloading exps.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/deltas', methods=['POST'])
def upload_deltas():
    """Upload deltas.csv file"""
    try:
        # Check password
        password = request.form.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Read and validate the CSV with dtype optimization
        df = pd.read_csv(file, dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str}, parse_dates=['update time'])
        required_columns = ['name', 'deltaexp', 'update time']
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': f'CSV must have columns: {required_columns}'}), 400
        
        # Backup existing file
        import shutil
        if os.path.exists(db.deltas_file):
            backup_file = db.deltas_file.replace('.csv', '_backup.csv')
            shutil.copy(db.deltas_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        records_count = len(df)
        df.to_csv(db.deltas_file, index=False)
        log_console(f"Uploaded deltas.csv with {records_count} records", "SUCCESS")
        
        del df
        gc.collect()
        return jsonify({'success': True, 'records': records_count})
    except Exception as e:
        log_console(f"Error uploading deltas.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/exps', methods=['POST'])
def upload_exps():
    """Upload exps.csv file"""
    try:
        # Check password
        password = request.form.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Read and validate the CSV with dtype optimization
        df = pd.read_csv(file, dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str}, parse_dates=['last update'])
        required_columns = ['name', 'exp', 'last update']
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': f'CSV must have columns: {required_columns}'}), 400
        
        # Backup existing file
        import shutil
        if os.path.exists(db.exps_file):
            backup_file = db.exps_file.replace('.csv', '_backup.csv')
            shutil.copy(db.exps_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        records_count = len(df)
        df.to_csv(db.exps_file, index=False)
        log_console(f"Uploaded exps.csv with {records_count} records", "SUCCESS")
        
        del df
        gc.collect()
        return jsonify({'success': True, 'records': records_count})
    except Exception as e:
        log_console(f"Error uploading exps.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/player-graph/<player_name>', methods=['GET'])
def get_player_graph(player_name):
    """Get individual player graph data"""
    datetime1 = request.args.get('datetime1')
    datetime2 = request.args.get('datetime2')

    try:
        graph_json = create_interactive_graph(player_name, db, datetime1, datetime2)
        return jsonify({'graph': graph_json, 'player': player_name})
    except Exception as e:
        log_console(f"Error generating player graph for {player_name}: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/player-details/<player_name>', methods=['GET'])
def get_player_details(player_name):
    """Get detailed player data from rubinothings.com.br and process VIP deltas if applicable"""
    try:
        player_data = scrape_player_data(player_name)
        
        # If scraping was successful and player is a VIP, update VIP data and calculate deltas
        if player_data.get('success') and player_data.get('tables'):
            vips = db.get_vips()
            matching_vip = next((v for v in vips if v['name'] == player_name), None)
            
            if matching_vip:
                # This is a VIP - process their data
                world = matching_vip['world']
                today_exp = 0
                today_online = 0
                
                for table in player_data['tables']:
                    columns = table['columns']
                    data = table['data']
                    
                    # Check for Raw XP table
                    if 'Raw XP no dia' in columns and data:
                        idx = columns.index('Raw XP no dia')
                        raw_value = data[0][idx] if len(data[0]) > idx else "0"
                        today_exp = int(raw_value.replace(',', '').replace('.', ''))
                    
                    # Check for Online time table
                    if 'Online time' in columns and data:
                        idx = columns.index('Online time')
                        online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                        today_online = parse_online_time_to_minutes(online_time_str)
                
                # Get OLD values from vipsdata BEFORE updating
                vipsdata = db.get_vipsdata()
                existing_vip = vipsdata[(vipsdata['name'] == player_name) & (vipsdata['world'] == world)]
                
                if not existing_vip.empty:
                    # VIP exists - calculate delta from previous cumulative
                    old_exp = existing_vip['today_exp'].values[0]
                    old_online = existing_vip['today_online'].values[0]
                    delta_exp = today_exp - old_exp
                    delta_online = today_online - old_online
                    
                    # Only process if exp has changed
                    if delta_exp != 0:
                        now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                        today_date = now.strftime("%Y-%m-%d")
                        db.add_vip_delta(player_name, world, today_date, delta_exp, delta_online, now)
                        log_console(f"VIP delta processed via player-details: {player_name} +{delta_exp} exp, +{delta_online} online", "INFO")
                        
                        # Update VIP data with NEW values
                        db.update_vipdata(player_name, world, today_exp, today_online)
                
                # Note: If exp hasn't changed, we don't update vipsdata or create delta
        
        return jsonify(player_data)
    except Exception as e:
        log_console(f"Error getting player details for {player_name}: {str(e)}", "ERROR")
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/delta', methods=['GET'])
def get_deltas():
    """Get recent delta updates for polling"""
    try:
        limit = request.args.get('limit', 100, type=int)
        world = request.args.get('world')
        guild = request.args.get('guild')
        
        all_deltas = db.get_deltas()
        
        if all_deltas.empty:
            return jsonify({'deltas': []})
        
        # Filter by world and guild if specified
        if world:
            all_deltas = all_deltas[all_deltas['world'] == world]
        if guild:
            all_deltas = all_deltas[all_deltas['guild'] == guild]
        
        if all_deltas.empty:
            return jsonify({'deltas': []})
        
        recent_deltas = all_deltas.sort_values(['update time', 'name'], ascending=[False, True]).head(limit)

        # Get distinct update times for efficient lookup (already returns array, no need for list conversion)
        distinct_times_list = sorted(all_deltas['update time'].unique())

        # Create a mapping of update_time -> prev_update_time
        prev_time_map = {}
        for i, current_time in enumerate(distinct_times_list):
            if i > 0:
                prev_time_map[current_time] = distinct_times_list[i - 1]
            else:
                prev_time_map[current_time] = current_time

        # Build delta list with calculated previous update times
        deltas = []
        for row in recent_deltas.itertuples(index=False):
            # Column 'update time' has space, use index position or getattr
            current_time = getattr(row, 'update_time', row[2])  # 'update time' is 3rd column
            prev_update_time = prev_time_map.get(current_time, current_time)

            deltas.append({
                'name': row.name,
                'deltaexp': int(row.deltaexp),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'world': getattr(row, 'world', DEFAULT_WORLD),
                'guild': getattr(row, 'guild', DEFAULT_GUILD)
            })
        
        response = jsonify({'deltas': deltas})
        del all_deltas, recent_deltas, distinct_times_list, prev_time_map, deltas
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error getting deltas: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/status-data')
def get_status_data():
    """Get the stored status data from all worlds"""
    try:
        status_data = db.get_status_data()
        if status_data:
            return jsonify(status_data)
        else:
            return jsonify({
                'error': 'No status data available yet',
                'message': 'Status data will be available after the first update'
            }), 404
    except Exception as e:
        log_console(f"Error getting status data: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scraping-config', methods=['GET'])
def get_scraping_config():
    """Get the scraping configuration"""
    try:
        config = db.get_scraping_config()
        return jsonify(config)
    except Exception as e:
        log_console(f"Error getting scraping config: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scraping-config', methods=['POST'])
def update_scraping_config():
    """Update the scraping configuration"""
    try:
        # Check password
        password = request.json.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        config = request.json.get('config')
        if not config:
            return jsonify({'error': 'No config provided'}), 400
        
        # Validate config structure
        if not isinstance(config, list):
            return jsonify({'error': 'Config must be an array'}), 400
        
        for item in config:
            if 'world' not in item or 'guilds' not in item:
                return jsonify({'error': 'Each config item must have "world" and "guilds" fields'}), 400
            if not isinstance(item['guilds'], list):
                return jsonify({'error': '"guilds" must be an array'}), 400
        
        # Save the configuration
        db.save_scraping_config(config)
        log_console(f"Scraping configuration updated via API", "SUCCESS")
        
        return jsonify({'success': True, 'config': config})
    except Exception as e:
        log_console(f"Error updating scraping config: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manual-update', methods=['POST'])
def manual_update():
    """Manually trigger a ranking update"""
    global scraper_state
    
    # Check if scraper is already active (not sleeping or idle)
    with scraper_lock:
        current_state = scraper_state
    
    if current_state in ['checking', 'scraping']:
        return jsonify({
            'success': False,
            'message': 'Scraper is already running',
            'state': current_state
        }), 409
    
    try:
        log_console("Manual update triggered", "INFO")
        
        with scraper_lock:
            scraper_state = "checking"
        
        # Get scraping configuration
        scraping_config = db.get_scraping_config()
        first_world = scraping_config[0]['world'] if scraping_config else DEFAULT_WORLD
        
        current_update = return_last_update(first_world, save_all_data=True, database=db)
        
        if current_update is None:
            raise Exception("Failed to get update time")
        
        with scraper_lock:
            scraper_state = "scraping"
        
        # Scrape all worlds and guilds
        all_players = []
        for config_item in scraping_config:
            world = config_item['world']
            guilds = config_item['guilds']
            
            for guild in guilds:
                try:
                    log_console(f"Manual scraping {world} - {guild}", "INFO")
                    r = get_ranking(world=world, guildname=guild)
                    
                    if r is None or len(r) < 2:
                        log_console(f"No data for {world} - {guild}", "WARNING")
                    else:
                        rankings = r[1]
                        rankparsed = parse_to_db_formatted(rankings, current_update, world=world, guild=guild)
                        all_players.append(rankparsed)
                except Exception as e:
                    log_console(f"Error scraping {world} - {guild}: {str(e)}", "ERROR")
        
        # Combine all players and update database
        if all_players:
            combined_df = pd.concat(all_players, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['name'], keep='first')
            db.update(combined_df, current_update)
            db.save()
            log_console(f"Manual update: {len(combined_df)} total players", "SUCCESS")
        else:
            raise Exception("No player data collected from any world/guild")
        
        with scraper_lock:
            scraper_state = "idle"
        
        log_console(f"Manual update completed successfully at {current_update}", "SUCCESS")
        return jsonify({
            'success': True,
            'message': 'Update completed successfully',
            'update_time': current_update.isoformat()
        })
    except Exception as e:
        error_msg = str(e)
        log_console(f"Manual update failed: {error_msg}", "ERROR")
        
        with scraper_lock:
            scraper_state = "idle"
        
        return jsonify({
            'success': False,
            'message': f'Update failed: {error_msg}'
        }), 500


@app.route('/vip')
def vip_page():
    """VIP tracking page"""
    return render_template('vip.html')


@app.route('/api/vip/list', methods=['GET'])
def get_vip_list():
    """Get list of VIP players"""
    try:
        vips = db.get_vips()
        return jsonify({'vips': vips})
    except Exception as e:
        log_console(f"Error getting VIP list: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/vip/add', methods=['POST'])
def add_vip():
    """Add a VIP player and immediately scrape their data"""
    try:
        data = request.json
        name = data.get('name')
        world = data.get('world')
        
        if not name or not world:
            return jsonify({'error': 'Name and world are required'}), 400
        
        success = db.add_vip(name, world)
        if success:
            # Immediately scrape the new VIP's data
            log_console(f"Immediately scraping new VIP: {name} ({world})", "INFO")
            scrape_single_vip(db, name, world)
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'VIP already exists'}), 400
    except Exception as e:
        log_console(f"Error adding VIP: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/vip/remove', methods=['POST'])
def remove_vip():
    """Remove a VIP player"""
    try:
        data = request.json
        name = data.get('name')
        world = data.get('world')
        
        if not name or not world:
            return jsonify({'error': 'Name and world are required'}), 400
        
        success = db.remove_vip(name, world)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'VIP not found'}), 404
    except Exception as e:
        log_console(f"Error removing VIP: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/vip/deltas', methods=['GET'])
def get_vip_deltas():
    """Get VIP delta history for live feed"""
    try:
        limit = request.args.get('limit', 100, type=int)
        name = request.args.get('name')
        world = request.args.get('world')
        
        deltavip = db.get_deltavip()
        
        if deltavip.empty:
            return jsonify({'deltas': []})
        
        # Keep original data before filtering for time header calculation
        original_deltavip = deltavip.copy()
        
        # Filter by name and world if provided
        if name:
            deltavip = deltavip[deltavip['name'] == name]
            original_deltavip = original_deltavip[original_deltavip['name'] == name]
        if world:
            deltavip = deltavip[deltavip['world'] == world]
            original_deltavip = original_deltavip[original_deltavip['world'] == world]
        
        # Filter out zero exp entries for display
        deltavip = deltavip[deltavip['delta_exp'] != 0]
        
        if deltavip.empty:
            return jsonify({'deltas': []})
        
        # Sort by update time descending and limit
        recent_deltas = deltavip.sort_values('update_time', ascending=False).head(limit)
        
        # Build delta list with time headers from last zero
        deltas = []
        for row in recent_deltas.itertuples(index=False):
            current_time = row[5]  # update_time is 6th column
            current_name = row.name
            current_world = row.world
            
            # Find the last zero delta before this entry
            vip_history = original_deltavip[
                (original_deltavip['name'] == current_name) & 
                (original_deltavip['world'] == current_world) &
                (original_deltavip['update_time'] < current_time)
            ].sort_values('update_time', ascending=False)
            
            # Get the most recent zero or the previous entry
            if not vip_history.empty:
                last_entry = vip_history.iloc[0]
                if last_entry['delta_exp'] == 0:
                    prev_update_time = last_entry['update_time']
                else:
                    # No recent zero, use the previous non-zero entry's time
                    prev_update_time = last_entry['update_time']
            else:
                # First entry, use current time
                prev_update_time = current_time
            
            deltas.append({
                'name': current_name,
                'world': current_world,
                'delta_exp': int(row.delta_exp),
                'delta_online': int(row.delta_online),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'date': row.date
            })
        
        response = jsonify({'deltas': deltas})
        del deltavip, original_deltavip, recent_deltas, deltas
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error getting VIP deltas: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/vip/graph', methods=['POST'])
def get_vip_graph():
    """Generate combined VIP graph with exp (bars) and online time (line)"""
    try:
        data = request.json
        name = data.get('name')
        world = data.get('world')
        
        if not name or not world:
            return jsonify({'error': 'Name and world are required'}), 400
        
        deltavip = db.get_deltavip()
        vip_data = deltavip[(deltavip['name'] == name) & (deltavip['world'] == world)]
        
        if vip_data.empty:
            return jsonify({'error': 'No data available for this VIP'}), 404
        
        # Sort by update time
        vip_data = vip_data.sort_values('update_time')
        
        # Apply zero grouping logic (compress consecutive zeros)
        all_update_times = vip_data['update_time'].tolist()
        exp_values = vip_data['delta_exp'].tolist()
        online_values = vip_data['delta_online'].tolist()
        
        # Identify positions where BOTH exp AND online are zero
        all_zero_positions = []
        for i in range(len(exp_values)):
            if exp_values[i] == 0 and online_values[i] == 0:
                all_zero_positions.append(i)
        
        # Group consecutive zero positions (only if there are 2+ consecutive zeros)
        zero_groups = []
        if all_zero_positions:
            start = all_zero_positions[0]
            for i in range(1, len(all_zero_positions)):
                if all_zero_positions[i] != all_zero_positions[i-1] + 1:
                    if all_zero_positions[i-1] - start >= 1:
                        zero_groups.append((start, all_zero_positions[i-1]))
                    start = all_zero_positions[i]
            if all_zero_positions[-1] - start >= 1:
                zero_groups.append((start, all_zero_positions[-1]))
        
        # Build compressed data with duplicate detection
        time_labels = []
        compressed_exp = []
        compressed_online = []
        compressed_online_display = []  # For display with time diff labels
        time_diffs = []  # Store time differences in minutes
        label_metadata = []  # Store (label, full_label, index) for duplicate detection
        
        prev_time = None
        prev_timestamp = None
        i = 0
        while i < len(all_update_times):
            # Check if this position starts a zero group
            in_zero_group = False
            for start, end in zero_groups:
                if i == start:
                    # Create label for zero period
                    if start > 0:
                        start_time = pd.to_datetime(all_update_times[start - 1])
                    else:
                        start_time = pd.to_datetime(all_update_times[start])
                    end_time = pd.to_datetime(all_update_times[end])
                    
                    start_date = start_time.date()
                    end_date = end_time.date()
                    
                    if start_date == end_date:
                        # Same day - short label without date
                        short_label = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
                        full_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%H:%M')}"
                    else:
                        # Different days - always show both full dates
                        short_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%d/%m/%Y %H:%M')}"
                        full_label = short_label
                    
                    label_metadata.append((short_label, full_label, len(time_labels)))
                    time_labels.append(short_label)
                    compressed_exp.append(0)
                    compressed_online.append(0)
                    compressed_online_display.append("0 / 0 min")
                    time_diffs.append(0)
                    prev_time = end_time
                    prev_timestamp = end_time
                    
                    i = end + 1
                    in_zero_group = True
                    break
            
            if not in_zero_group:
                time_obj = pd.to_datetime(all_update_times[i])
                current_date = time_obj.date()
                
                # Calculate time difference from previous timestamp
                if prev_timestamp is not None:
                    time_diff_minutes = int((time_obj - prev_timestamp).total_seconds() / 60)
                else:
                    time_diff_minutes = 0
                
                if prev_time is None:
                    # First data point - show as single time point
                    short_label = time_obj.strftime('%H:%M')
                    full_label = time_obj.strftime('%d/%m/%Y %H:%M')
                else:
                    start_date = prev_time.date()
                    if start_date == current_date:
                        # Same day - short label without date
                        short_label = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                        full_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                    else:
                        # Different days - always show both full dates
                        short_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                        full_label = short_label
                
                label_metadata.append((short_label, full_label, len(time_labels)))
                time_labels.append(short_label)
                compressed_exp.append(exp_values[i])
                
                # If online time is 0 but exp is not 0, use None for interpolation
                online_val = online_values[i]
                if online_val == 0 and exp_values[i] > 0:
                    compressed_online.append(None)  # Will be interpolated by Plotly
                else:
                    compressed_online.append(online_val)
                
                # Create display label: "online / time_diff min"
                display_label = f"{online_val} / {time_diff_minutes} min"
                compressed_online_display.append(display_label)
                time_diffs.append(time_diff_minutes)
                
                prev_time = time_obj
                prev_timestamp = time_obj
                i += 1
        
        # Check for duplicates and replace with full labels where needed
        from collections import Counter
        label_counts = Counter(time_labels)
        for short_label, full_label, idx in label_metadata:
            if label_counts[short_label] > 1:
                time_labels[idx] = full_label
        
        # Create combined graph with dual y-axes
        fig = go.Figure()
        
        # Add EXP bars (left y-axis)
        fig.add_trace(go.Bar(
            x=time_labels,
            y=compressed_exp,
            name='EXP Gain',
            marker_color='#C21500',
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_exp],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>',
            yaxis='y'
        ))
        
        # Add smooth line connecting the tops of the EXP bars
        fig.add_trace(go.Scatter(
            x=time_labels,
            y=compressed_exp,
            name='EXP Trend',
            mode='lines',
            line=dict(color='#C21500', width=2, shape='spline'),
            showlegend=False,
            hoverinfo='skip',
            yaxis='y'
        ))
        
        # Add Online Time line (right y-axis)
        fig.add_trace(go.Scatter(
            x=time_labels,
            y=compressed_online,
            name='Online Time (min)',
            mode='lines+markers',
            line=dict(color='#3498db', width=2, shape='spline'),  # Use spline for smooth interpolation
            marker=dict(size=8, symbol='circle'),
            text=compressed_online_display,
            hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
            connectgaps=True,  # Connect gaps where None values exist (interpolation)
            yaxis='y2'
        ))
        
        # Update layout with dual y-axes
        fig.update_layout(
            title=f'🌟 {name} - VIP Stats ({world})',
            xaxis_title='Update Time',
            yaxis=dict(
                title=dict(text='Delta EXP', font=dict(color='#C21500')),
                tickfont=dict(color='#C21500')
            ),
            yaxis2=dict(
                title=dict(text='Online Time (minutes)', font=dict(color='#3498db')),
                tickfont=dict(color='#3498db'),
                overlaying='y',
                side='right'
            ),
            template='plotly_white',
            height=500,
            xaxis=dict(tickangle=-45),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode='x unified'
        )
        
        result = jsonify({
            'success': True,
            'graph_data': fig.to_json(),
            'stats': {
                'total_exp': int(vip_data['delta_exp'].sum()),
                'avg_exp': float(vip_data['delta_exp'].mean()),
                'max_exp': int(vip_data['delta_exp'].max()),
                'total_online': int(vip_data['delta_online'].sum()),
                'avg_online': float(vip_data['delta_online'].mean()),
                'updates': len(vip_data)
            }
        })
        del vip_data, fig, time_labels, compressed_exp, compressed_online, compressed_online_display, time_diffs
        gc.collect()
        return result
    except Exception as e:
        log_console(f"Error generating VIP graph: {str(e)}", "ERROR")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
